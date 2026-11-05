"""本地二进制镜像：把 electron-builder 的 toolset 请求转到 npmmirror。

为什么需要它：这台机器访问 GitHub releases 要么 502（走代理）要么超时（直连），
electron-builder 26 的 winCodeSign / nsis bundle 都从那下载，构建必然失败。
npmmirror 上有同样的文件（sha256 与 electron-builder 内置校验值一致），
但这个目录结构带 `@` 和特殊路径，直接配 ELECTRON_BUILDER_BINARIES_MIRROR
到 npmmirror 时 electron-builder 仍会走 GitHub（实测两次都打 20.205.243.166）。

于是这里做个中间层：请求进来先看本地 cache 有没有，没有再转发到 npmmirror，
并把每一行请求打到 stdout —— 这样即使版本号猜错，也能从日志里看到真实路径。
"""
import os
import sys
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CACHE = os.path.expandvars(r"%LOCALAPPDATA%\electron-builder\Cache")
UPSTREAM = "https://npmmirror.com/mirrors/electron-builder-binaries/"
PROXY = "http://127.0.0.1:62437"


def _opener():
    h = urllib.request.ProxyHandler({"http": PROXY, "https": PROXY})
    return urllib.request.build_opener(h)


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        rel = urllib.parse.unquote(self.path.lstrip("/"))
        print("REQ %s" % rel, flush=True)
        local = os.path.join(CACHE, rel.replace("/", os.sep))
        if os.path.isfile(local):
            data = open(local, "rb").read()
            print("  HIT  %s (%d bytes)" % (local, len(data)), flush=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        print("  MISS → 转发上游", flush=True)
        try:
            with _opener().open(UPSTREAM + rel, timeout=120) as r:
                data = r.read()
            print("  UPSTREAM %d bytes" % len(data), flush=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            print("  UPSTREAM 失败: %s" % e, flush=True)
            self.send_error(404, str(e))

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8899
    print("本地二进制镜像 → http://127.0.0.1:%d/  cache=%s" % (port, CACHE), flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
