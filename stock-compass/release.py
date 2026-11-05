"""生成更新清单 latest.json（发布用）。

自动更新只认清单，不认目录里有什么文件。清单长这样：

    {
      "version": "1.0.1",
      "url": "RightsideCompass-1.0.1-portable.exe",
      "sha256": "…",
      "size": 81500000,
      "notes": "修复了什么",
      "force": false
    }

`url` 可以写相对文件名 —— 客户端会相对 update.json 里的 feedUrl 解析，
所以换域名不用改清单。

用法
----
    python release.py                                   # 用 desktop/package.json 的版本号
    python release.py --version 1.0.1 --notes "修复X"    # 指定版本与更新说明
    python release.py --publish D:\\update-server        # 顺便把 exe 拷到发布目录
    python release.py --force                            # 标记为强制更新
"""
import argparse
import hashlib
import json
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DIST = os.path.join(HERE, "desktop", "dist")
PKG = os.path.join(HERE, "desktop", "package.json")


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_version():
    try:
        with open(PKG, encoding="utf-8") as f:
            return json.load(f).get("version")
    except OSError:
        return None


def find_exe(explicit=None):
    if explicit:
        return explicit if os.path.isfile(explicit) else None
    if not os.path.isdir(DIST):
        return None
    cands = [os.path.join(DIST, n) for n in os.listdir(DIST)
             if n.lower().endswith(".exe") and "portable" in n.lower()]
    if not cands:
        cands = [os.path.join(DIST, n) for n in os.listdir(DIST) if n.lower().endswith(".exe")]
    if not cands:
        return None
    return max(cands, key=os.path.getmtime)


def main():
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--file", help="要发布的 exe（默认取 desktop/dist 里最新的 portable 包）")
    ap.add_argument("--version", help="版本号（默认读 desktop/package.json）")
    ap.add_argument("--notes", default="", help="更新说明，显示在更新提示里")
    ap.add_argument("--force", action="store_true", help="标记为强制更新")
    ap.add_argument("--publish", help="把 exe 和 latest.json 复制到这个目录（更新服务器根目录）")
    ap.add_argument("--url", help="清单里的 url（默认只写文件名）")
    a = ap.parse_args()

    exe = find_exe(a.file)
    if not exe:
        print("找不到要发布的 exe。先执行 npm run dist 打包，或用 --file 指定。")
        return 1

    version = a.version or read_version()
    if not version:
        print("拿不到版本号：desktop/package.json 缺少 version，且没传 --version。")
        return 1

    name = os.path.basename(exe)
    digest = sha256_of(exe)
    size = os.path.getsize(exe)

    manifest = {
        "version": version,
        "url": a.url or name,
        "sha256": digest,
        "size": size,
        "notes": a.notes,
        "force": bool(a.force),
    }

    targets = [DIST]
    if a.publish:
        os.makedirs(a.publish, exist_ok=True)
        targets.append(a.publish)
        dst_exe = os.path.join(a.publish, name)
        if os.path.abspath(dst_exe) != os.path.abspath(exe):
            shutil.copy2(exe, dst_exe)
            print("已复制:", dst_exe)

    for d in targets:
        p = os.path.join(d, "latest.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print("已写入:", p)

    print("\n  版本   :", version)
    print("  文件   :", name, "(%.1f MB)" % (size / 1048576))
    print("  sha256 :", digest)
    print("\n把这个目录挂成静态服务，再在 update.json 里把 feedUrl 指过去即可。")
    print('例如： python -m http.server 8099 --directory "%s"' % (a.publish or DIST))
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
