#!/usr/bin/env python3
"""
echo_bytes.py —— 以文件为载荷的字节级回显校验（压测 / 回归两用）

为什么不用 "A" * N：
  1. 全同字节掩盖不了"错位"——少发一段、多发一段、重复一段都还是 A，校验不出来
  2. 真实文本有 UTF-8 多字节序列，能顺带验证缓冲区没有按字节切断
  3. hamlet 199,813 B 是 16 KB 内核发送缓冲的 12 倍，天生必触发 EAGAIN，
     不需要靠 --size 硬凑

默认载荷：test/hamlet（199,813 B / 7082 行）

用法：
  # 单连接，原样回显校验
  python3 test/echo_bytes.py 6471

  # 重复 8 次约 1.6 MB，客户端先憋 2 秒制造服务端 EAGAIN
  python3 test/echo_bytes.py 6471 --repeat=8 --noread=2

  # 指定别的文件
  python3 test/echo_bytes.py 6471 --file=/etc/services --repeat=20

退出码：
  0  字节级完全一致
  1  长度或内容不一致
  2  连接/超时等运行错误
"""

import argparse
import hashlib
import socket
import sys
import threading
import time
from pathlib import Path

# 项目根 = 本脚本所在目录的上一级
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FILE = ROOT / "test" / "hamlet"

RECV_CHUNK = 65536


def load_payload(path: Path, repeat: int) -> bytes:
    if not path.exists():
        sys.exit(f"[fatal] 载荷文件不存在: {path}")
    raw = path.read_bytes()
    if not raw:
        sys.exit(f"[fatal] 载荷文件为空: {path}")
    return raw * repeat


def first_diff(got: bytes, want: bytes) -> str:
    """返回第一处差异的可读描述，一致时返回空串。"""
    n = min(len(got), len(want))
    for i in range(n):
        if got[i] != want[i]:
            return (f"偏移 {i} (0x{i:x}) 处不同: "
                    f"期望 0x{want[i]:02x}，实际 0x{got[i]:02x}")
    if len(got) < len(want):
        return f"少了 {len(want) - len(got)} 字节（收到 {len(got)} / 期望 {len(want)}）"
    if len(got) > len(want):
        return f"多了 {len(got) - len(want)} 字节（收到 {len(got)} / 期望 {len(want)}）"
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="以文件为载荷的 echo 字节级校验",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("port", type=int, help="服务端端口")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--file", default=str(DEFAULT_FILE),
                    help=f"载荷文件（默认 {DEFAULT_FILE.name}）")
    ap.add_argument("--repeat", type=int, default=1,
                    help="把载荷重复 N 次（默认 1）")
    ap.add_argument("--rcvbuf", type=int, default=0,
                    help="SO_RCVBUF，0 = 系统默认；压小可逼出服务端 EAGAIN")
    ap.add_argument("--noread", type=float, default=0.0,
                    help="发完静默 N 秒再开始读")
    ap.add_argument("--timeout", type=float, default=180.0,
                    help="接收总超时（秒），默认 180")
    ap.add_argument("--stall-limit", type=float, default=30.0,
                    help="连续 N 秒零进展即判定卡死（秒），默认 30")
    args = ap.parse_args()

    payload = load_payload(Path(args.file), args.repeat)
    total = len(payload)

    print(f"[input] 文件     = {args.file}")
    print(f"[input] 重复     = {args.repeat} 次")
    print(f"[input] 总字节   = {total:,}")
    if args.rcvbuf:
        print(f"[input] rcvbuf   = {args.rcvbuf}")
    if args.noread:
        print(f"[input] 预静默   = {args.noread} 秒（逼服务端走 wbuf/EPOLLOUT）")

    try:
        s = socket.create_connection((args.host, args.port), timeout=10)
    except OSError as e:
        print(f"[fatal] 连接 {args.host}:{args.port} 失败: {e}")
        return 2

    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if args.rcvbuf:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.rcvbuf)

    send_state = {}

    def sender():
        try:
            s.sendall(payload)
            send_state["ok"] = True
        except OSError as e:
            send_state["err"] = repr(e)

    t = threading.Thread(target=sender, daemon=True)
    t.start()

    if args.noread > 0:
        time.sleep(args.noread)

    got = bytearray()
    deadline = time.time() + args.timeout
    s.settimeout(1.0)
    stalled = 0
    while len(got) < total and time.time() < deadline:
        try:
            chunk = s.recv(RECV_CHUNK)
        except socket.timeout:
            stalled += 1
            # 连续 N 秒毫无进展 -> 判定卡死，别白等到 timeout
            if stalled >= args.stall_limit and len(got) < total:
                print(f"[warn] 连续 {stalled:.0f} 秒零进展，判定卡死")
                break
            continue
        if not chunk:
            break
        got.extend(chunk)
        stalled = 0

    t.join(timeout=5)

    gb, wb = bytes(got), payload
    print(f"[recv ] 实际收到 = {len(gb):,}")

    if "err" in send_state:
        print(f"[warn ] 发送端异常: {send_state['err']}")

    diff = first_diff(gb, wb)
    if not diff:
        print(f"[ok   ] 字节级完全一致 {len(gb):,} B")
        print(f"[ok   ] sha256 = {hashlib.sha256(gb).hexdigest()[:16]}…")
        s.close()
        return 0

    print(f"[FAIL ] {diff}")
    if wb:
        print(f"[FAIL ] 放大倍数 = {len(gb) / len(wb):.4f}x")
    print(f"[FAIL ] sha256 期望 = {hashlib.sha256(wb).hexdigest()[:16]}…")
    print(f"[FAIL ] sha256 实际 = {hashlib.sha256(gb).hexdigest()[:16]}…")
    s.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())