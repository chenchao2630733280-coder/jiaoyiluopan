"""独立的更新器：等主程序退出 → 校验 → 替换 exe → 重启。

为什么必须是独立进程
--------------------
Windows 上不能替换正在运行的 exe（文件被独占锁）。主程序能做的只有三件事：
下载、校验、把「新文件路径」交给别人。真正落盘的活儿必须等它退出之后，
由另一个进程完成 —— 这就是 WorkBuddy 安装目录里那个 updater.exe 的职责，
Electron 生态里是 Squirrel / NSIS 的 update helper，都是同一套道理。

这里用 Python 实现（纯标准库），由主进程以 detached 方式拉起，参数：

    --pid 12345                 等这个进程退出
    --src  C:\\temp\\new.exe     新文件
    --dst  D:\\app\\app.exe      要替换的文件
    --sha256 <hex>              新文件的 sha256（不匹配就不动，避免把好文件换坏）
    --restart                   替换后启动 dst
    --log  D:\\app\\update.log   追加日志

它自己不需要被删除（脚本文件，不是 exe），失败了也不影响主程序 —— 主程序在
拉起它之前已经把新文件校验过一遍，这里再校验一次是防「下载后又被改动」。
"""
import ctypes
import hashlib
import os
import shutil
import subprocess
import sys
import time

PROCESS_SYNCHRONIZE = 0x00100000
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

LOG = []


def log(msg):
    line = "[%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    LOG.append(line)
    print(line, flush=True)
    path = _arg("--log")
    if path:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


def _arg(name, default=None):
    argv = sys.argv[1:]
    if name in argv:
        i = argv.index(name)
        if i + 1 < len(argv):
            return argv[i + 1]
    return default


def _flag(name):
    return name in sys.argv[1:]


def wait_pid(pid, timeout=180):
    """等主进程真正退出。只等 pid 还不够 —— 它的子进程可能还持有文件句柄，
    所以退出后额外再等一会儿，让系统把句柄回收掉。"""
    if not pid:
        return
    h = kernel32.OpenProcess(PROCESS_SYNCHRONIZE, False, int(pid))
    if not h:
        log("拿不到进程句柄（可能已退出），继续")
        return
    try:
        log("等待主进程 %s 退出 …" % pid)
        rc = kernel32.WaitForSingleObject(h, timeout * 1000)
        log("主进程已退出（等待返回 %d）" % rc)
    finally:
        kernel32.CloseHandle(h)
    time.sleep(1.5)


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def replace_with_retry(src, dst, tries=40, gap=0.5):
    """把 src 覆盖到 dst。

    即便主进程退出了，explorer/杀软/索引仍可能短暂持有 dst 的句柄，
    所以这里用「备份旧文件 → 复制新文件」并且整段重试，
    而不是指望一次 os.replace 就成功。
    """
    bak = dst + ".bak"
    for i in range(tries):
        try:
            if os.path.exists(bak):
                try:
                    os.remove(bak)
                except OSError:
                    pass
            if os.path.exists(dst):
                os.replace(dst, bak)
            shutil.copy2(src, dst)
            log("替换成功（第 %d 次尝试）" % (i + 1))
            return True
        except OSError as e:
            # 失败时尽量把旧文件放回去，别留下一个缺失的程序
            if not os.path.exists(dst) and os.path.exists(bak):
                try:
                    os.replace(bak, dst)
                except OSError:
                    pass
            if i == 0 or i % 5 == 0:
                log("替换重试 %d/%d：%s" % (i + 1, tries, e))
            time.sleep(gap)
    return False


def launch(exe):
    try:
        subprocess.Popen([exe], close_fds=True)
        log("已启动: %s" % exe)
        return True
    except OSError as e:
        log("启动失败：%s（请手动打开）" % e)
        return False


def swap_and_launch(src, dst):
    """便携包专用：新版是**另一个文件名**（RightsideCompass-1.0.1-portable.exe）。

    便携包每次运行都会把自己解压到临时目录，所以 process.execPath 不是用户看到的
    那个 exe；真正要「换掉」的是 exe 旁边的那个文件。文件名带版本号 → 不存在覆盖问题，
    只需要启动新版，并尽力删掉旧版（删不掉也无害，用户看版本号就知道该用哪个）。
    """
    if not launch(src):
        return 5
    if dst and os.path.exists(dst) and os.path.abspath(src) != os.path.abspath(dst):
        for _ in range(10):
            try:
                os.remove(dst)
                log("已删除旧版本: %s" % dst)
                return 0
            except OSError:
                time.sleep(0.5)
        log("旧版本删不掉（可能仍被占用），保留：%s" % dst)
    return 0


def main():
    pid = _arg("--pid")
    src = _arg("--src")
    dst = _arg("--dst")
    digest = _arg("--sha256")
    mode = _arg("--mode", "replace")

    log("=" * 60)
    log("更新器启动：mode=%s src=%s dst=%s" % (mode, src, dst))
    if not src or not dst:
        log("参数不完整，退出")
        return 2
    if not os.path.isfile(src):
        log("新文件不存在，退出")
        return 2

    wait_pid(pid)

    if digest:
        actual = sha256_of(src)
        if actual.lower() != digest.lower():
            log("sha256 不匹配，放弃更新")
            log("  期望 %s" % digest)
            log("  实际 %s" % actual)
            return 3
        log("sha256 校验通过")

    if mode == "swap":
        return swap_and_launch(src, dst)

    if not replace_with_retry(src, dst):
        log("替换失败，程序保持原样（新文件留在 %s）" % src)
        return 4

    if _flag("--restart"):
        launch(dst)

    log("更新完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
