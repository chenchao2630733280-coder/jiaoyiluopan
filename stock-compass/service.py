"""把罗盘后端注册成 Windows 服务（纯标准库，不需要 pywin32）。

为什么需要它
------------
桌面端是个窗口程序：关掉窗口，Electron 会连带把 Python 后端停掉（这是桌面客户端的
正确行为，不然会留孤儿进程）。但行情数据是要**一直跟着交易日走**的 —— 收盘后要补扫、
第二天开盘前要切到新的 K 线。如果只有窗口在的时候才更新，你打开软件看到的就是昨天的数据。

于是把它拆成两层，和 WorkBuddy 的做法一样：

    窗口（Electron）  —— 随时可以关，只负责显示
    CoreService       —— 常驻后台，负责抓数、扫描、算榜单，监听 127.0.0.1:8092

窗口启动时先探 8092：服务在跑就直接连（秒开、零扫描）；不在跑才自己拉一个临时后端。

实现方式
--------
不引入 pywin32 / nssm / srvany，直接用 ctypes 调 advapi32 的
StartServiceCtrlDispatcherW —— 这是 SCM 要求的入口。Python 进程只要实现了
「注册控制处理器 + 上报状态」这两件事，就能被 sc 当成正常服务管理（否则会报 1053）。

用法
----
    python service.py --install     安装并设为自动启动（需管理员）
    python service.py --uninstall   停止并删除服务（需管理员）
    python service.py --start       启动
    python service.py --stop        停止
    python service.py --status      查看状态
    python service.py --console     不用服务，前台跑一份（调试用）

    sc create 用的命令是 `python.exe service.py --run`，--run 不要手动执行。
"""
import ctypes
import os
import subprocess
import sys
import threading
import time
from ctypes import wintypes as w

SERVICE_NAME = "RightsideCompass.CoreService"
SERVICE_DISPLAY = "A股右侧交易罗盘 · 数据服务"
SERVICE_DESC = (
    "常驻后台抓取行情、扫描全市场并生成榜单。关闭桌面窗口后仍继续运行，"
    "窗口再次打开时直接复用本服务（秒开，无需重新扫描）。"
)
DEFAULT_PORT = 8092


# --- 服务相关常量（winnt.h）--------------------------------------------------
SERVICE_WIN32_OWN_PROCESS = 0x00000010
SERVICE_ACCEPT_STOP = 0x00000001
SERVICE_ACCEPT_SHUTDOWN = 0x00000004
SERVICE_CONTROL_STOP = 0x00000001
SERVICE_CONTROL_SHUTDOWN = 0x00000005

SERVICE_STOPPED = 0x00000001
SERVICE_START_PENDING = 0x00000002
SERVICE_STOP_PENDING = 0x00000003
SERVICE_RUNNING = 0x00000004

ERROR_FAILED_SERVICE_CONTROLLER_CONNECT = 1063

advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)


class SERVICE_STATUS(ctypes.Structure):
    _fields_ = [
        ("dwServiceType", w.DWORD),
        ("dwCurrentState", w.DWORD),
        ("dwControlsAccepted", w.DWORD),
        ("dwWin32ExitCode", w.DWORD),
        ("dwServiceSpecificExitCode", w.DWORD),
        ("dwCheckPoint", w.DWORD),
        ("dwWaitHint", w.DWORD),
    ]


class SERVICE_TABLE_ENTRYW(ctypes.Structure):
    _fields_ = [("lpServiceName", ctypes.c_wchar_p), ("lpServiceProc", ctypes.c_void_p)]


SERVICE_MAIN_FUNCTION = ctypes.WINFUNCTYPE(None, w.DWORD, ctypes.POINTER(w.LPWSTR))
HANDLER_EX = ctypes.WINFUNCTYPE(w.DWORD, w.DWORD, w.DWORD, ctypes.c_void_p, ctypes.c_void_p)

advapi32.StartServiceCtrlDispatcherW.restype = w.BOOL
advapi32.StartServiceCtrlDispatcherW.argtypes = [ctypes.POINTER(SERVICE_TABLE_ENTRYW)]
advapi32.RegisterServiceCtrlHandlerExW.restype = w.HANDLE
advapi32.RegisterServiceCtrlHandlerExW.argtypes = [w.LPCWSTR, HANDLER_EX, ctypes.c_void_p]
advapi32.SetServiceStatus.restype = w.BOOL
advapi32.SetServiceStatus.argtypes = [w.HANDLE, ctypes.POINTER(SERVICE_STATUS)]


# --- 服务宿主 ---------------------------------------------------------------

_status_handle = None
_stop_evt = threading.Event()
_backend = None          # server 模块（延迟导入，见 _run_backend）


def _report(state, exit_code=0, wait_hint=0, checkpoint=0):
    """向 SCM 上报状态。不报的话服务管理器会认为进程卡死（1053）。"""
    if not _status_handle:
        return
    st = SERVICE_STATUS()
    st.dwServiceType = SERVICE_WIN32_OWN_PROCESS
    st.dwCurrentState = state
    st.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN if state == SERVICE_RUNNING else 0
    st.dwWin32ExitCode = exit_code
    st.dwServiceSpecificExitCode = 0
    st.dwCheckPoint = checkpoint
    st.dwWaitHint = wait_hint
    advapi32.SetServiceStatus(_status_handle, ctypes.byref(st))


def _service_handler(control, event_type, event_data, context):
    """SCM 的控制回调：停止/关机时通知后端收尾。"""
    if control in (SERVICE_CONTROL_STOP, SERVICE_CONTROL_SHUTDOWN):
        _report(SERVICE_STOP_PENDING, wait_hint=8000)
        # shutdown() 会等 serve_forever 退出，不能在回调线程里直接等（会和 SCM 互相干等）
        threading.Thread(target=_shutdown_backend, daemon=True).start()
    return 0


def _service_dir():
    """服务运行时的可写目录。

    服务以 LocalSystem 身份跑，程序目录（Program Files / U 盘只读）不一定能写。
    ProgramData 是 Windows 给「所有用户共享的应用数据」准备的位置，权限足够。
    """
    base = os.environ.get("ProgramData") or r"C:\ProgramData"
    d = os.path.join(base, "RightsideCompass")
    os.makedirs(d, exist_ok=True)
    return d


def _run_backend():
    """在后台线程里跑 server.main()，与命令行启动走完全相同的代码路径。"""
    global _backend
    # ★ 必须在 import server 之前设好 COMPASS_STORE —— server / engine 是模块级读环境变量
    os.environ["COMPASS_STORE"] = _service_dir()
    os.environ.setdefault("PORT", str(DEFAULT_PORT))
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    import server as srv_mod
    _backend = srv_mod

    saved = sys.argv
    sys.argv = [saved[0]]          # 防止 service.py 的参数被 server.main() 当成端口号
    try:
        srv_mod.main()
    except SystemExit:
        pass
    except Exception as e:
        print("  [服务] 后端异常退出:", e)
    finally:
        sys.argv = saved


def _shutdown_backend():
    if _backend is None:
        _stop_evt.set()
        return
    s = getattr(_backend, "_SRV", [None])[0]
    if s is not None:
        try:
            s.shutdown()
        except Exception:
            pass
    _stop_evt.set()


def _service_main(argc, argv):
    global _status_handle
    _status_handle = advapi32.RegisterServiceCtrlHandlerExW(
        SERVICE_NAME, HANDLER_EX(_service_handler), None)
    if not _status_handle:
        return
    _report(SERVICE_START_PENDING, wait_hint=10000)
    threading.Thread(target=_run_backend, name="backend", daemon=True).start()

    # 等服务真的能响应再上报 RUNNING，否则「服务已启动」但页面打不开，很容易误判
    ok = _wait_port(DEFAULT_PORT, 60)
    if not ok:
        _report(SERVICE_STOPPED, exit_code=1)
        return
    _report(SERVICE_RUNNING)
    _stop_evt.wait()
    _report(SERVICE_STOPPED)


def _wait_port(port, timeout=60):
    import socket
    t0 = time.time()
    while time.time() - t0 < timeout:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        if _stop_evt.is_set():
            return False
        time.sleep(0.4)
    return False


def run_as_service():
    """入口：把进程交给 SCM。sc create 时 binPath 指向 `python.exe service.py --run`。"""
    cb = SERVICE_MAIN_FUNCTION(_service_main)
    table = (SERVICE_TABLE_ENTRYW * 2)()
    table[0].lpServiceName = SERVICE_NAME
    table[0].lpServiceProc = ctypes.cast(cb, ctypes.c_void_p)
    table[1].lpServiceName = None
    table[1].lpServiceProc = None
    if not advapi32.StartServiceCtrlDispatcherW(table):
        err = ctypes.get_last_error()
        if err == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT:
            print("这个命令只能由服务管理器启动。")
            print("  安装服务： python service.py --install")
            print("  前台调试： python service.py --console")
        else:
            print("连接服务管理器失败，错误码 %d。" % err)
        return 1
    return 0


# --- 安装 / 卸载 / 控制 ------------------------------------------------------

def _is_admin():
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _sc(*args):
    """调 sc.exe。用列表传参避开 bat 里那套引号地狱。

    ★ 注意：有些环境（企业安全策略、EDR）会把 sc.exe 列进程序黑名单，
    此时 subprocess 直接抛 OSError / PermissionError。这里必须接住，
    否则用户看到的是一段 Python traceback，根本不知道发生了什么。
    """
    try:
        r = subprocess.run(["sc"] + list(args), capture_output=True, text=True, encoding="mbcs")
    except OSError as e:
        return 1, (
            "无法调用 sc.exe：%s\n\n"
            "可能原因：\n"
            "  · 系统安全策略 / 杀软把 sc.exe 列入了黑名单；\n"
            "  · 当前用户没有权限（请以管理员身份运行 install_service.bat）。\n\n"
            "替代办法：打开「服务」管理器（services.msc）手动创建服务，\n"
            "  可执行文件填 python.exe 的完整路径，参数填 \"<程序目录>\\service.py\" --run"
            % e
        )
    return r.returncode, (r.stdout or "").strip() + (r.stderr or "").strip()


def _bin_path():
    """拼 sc 的 binPath 值。

    格式是 `"python.exe" "service.py" --run` —— 两条路径各自带引号，
    否则路径里有空格时（本机就有 `A股右侧交易罗盘` 这个中文目录）sc 会把
    后半截当成参数。交给 subprocess 的列表形式，CreateProcess 的规则会把
    内层引号转成 \"，sc 拿到的正是想要的那串。
    """
    return '"%s" "%s" --run' % (sys.executable, os.path.abspath(__file__))


def install():
    if not _is_admin():
        print("需要管理员权限。请右键 install_service.bat → 以管理员身份运行。")
        return 1
    code, out = _sc("create", SERVICE_NAME,
                    "binPath=", _bin_path(),
                    "start=", "auto",
                    "DisplayName=", SERVICE_DISPLAY)
    print(out)
    if code != 0 and "已存在" not in out and "already exists" not in out.lower():
        return code
    _sc("description", SERVICE_NAME, SERVICE_DESC)
    # 服务意外退出时自动重启（第一次 5 秒后，之后每分钟），行情服务不该静默死掉
    _sc("failure", SERVICE_NAME, "reset=", "86400", "actions=", "restart/5000/restart/60000")
    code, out = _sc("start", SERVICE_NAME)
    print(out)
    time.sleep(2)
    return status()


def uninstall():
    if not _is_admin():
        print("需要管理员权限。请右键 uninstall_service.bat → 以管理员身份运行。")
        return 1
    _sc("stop", SERVICE_NAME)
    time.sleep(2)
    code, out = _sc("delete", SERVICE_NAME)
    print(out)
    return code


def status():
    code, out = _sc("query", SERVICE_NAME)
    print(out)
    return 0 if code == 0 else code


def control(action):
    if not _is_admin() and action in ("start", "stop"):
        print("启动/停止服务需要管理员权限。")
        return 1
    code, out = _sc(action, SERVICE_NAME)
    print(out)
    return code


def run_console():
    """前台跑一份带真实服务数据的后端 —— 用来验证服务模式下的行为，不用装服务。"""
    os.environ["COMPASS_STORE"] = _service_dir()
    print("以服务数据目录前台运行（Ctrl+C 退出）")
    print("  数据目录:", os.environ["COMPASS_STORE"])
    _run_backend()
    return 0


def main():
    arg = (sys.argv[1] if len(sys.argv) > 1 else "").lower()
    if arg == "--run":
        return run_as_service()
    if arg == "--install":
        return install()
    if arg == "--uninstall":
        return uninstall()
    if arg == "--start":
        return control("start")
    if arg == "--stop":
        return control("stop")
    if arg == "--status":
        return status()
    if arg == "--console":
        return run_console()
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
