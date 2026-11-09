#!/usr/bin/env python3
"""
大文件回声一致性测试

用途：把整个文件推给 echo 服务端，把回来的字节流存盘，逐字节比对。
      任何"少了 / 多了 / 错位 / 顺序乱"都会在这里暴露出第一个差异位置。

用法:
    ./build/bench_server --server=epoll --port=6200 &
    python3 test/bigfile_echo.py 6200 /tmp/big_in.bin [--rcvbuf=8192] [--noread=0]

    --rcvbuf   客户端接收缓冲区（小 => 更快逼出服务端 send 的 EAGAIN）
    --noread   发完之后先不读，静默 N 秒，逼迫服务端 send buffer 塞满

输出: /tmp/big_out.bin
"""
import argparse
import os
import socket
import sys
import threading
import time


def make_data(nbytes: int) -> bytes:
    """生成带序号的可校验数据：每 64 字节一个大端序号 + '.' 填充。"""
    out = bytearray()
    i = 0
    while len(out) < nbytes:
        out += (b'%08d' % i).ljust(64, b'.')
        i += 1
    return bytes(out[:nbytes])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('port', type=int)
    ap.add_argument('--size', type=int, default=8 * 1024 * 1024,
                    help='发送字节数，默认 8MB')
    ap.add_argument('--file', default=None, help='从文件读数据；缺省则用生成的确定性数据')
    ap.add_argument('--rcvbuf', type=int, default=0, help='SO_RCVBUF，0 表示用系统默认')
    ap.add_argument('--noread', type=float, default=0.0, help='发完静默 N 秒再开始读')
    ap.add_argument('--timeout', type=float, default=40.0, help='接收总超时（秒）')
    args = ap.parse_args()

    if args.file:
        payload = open(args.file, 'rb').read()
    else:
        payload = make_data(args.size)
    total = len(payload)
    print(f'[test] payload = {total} bytes ({total / 1024 / 1024:.2f} MB)')

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if args.rcvbuf:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.rcvbuf)
        print(f'[test] SO_RCVBUF = {sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)}')
    sock.settimeout(10.0)
    sock.connect(('127.0.0.1', args.port))
    print(f'[test] connected -> 127.0.0.1:{args.port}')

    send_err = []

    def sender() -> None:
        try:
            sock.sendall(payload)
            print('[test] 发送完成')
        except Exception as exc:                      # noqa: BLE001
            send_err.append(exc)
            print(f'[test] 发送异常: {exc!r}')

    th = threading.Thread(target=sender, daemon=True)
    th.start()

    if args.noread > 0:
        print(f'[test] 静默 {args.noread}s 不读（逼服务端 send buffer 塞满）')
        time.sleep(args.noread)

    got = bytearray()
    deadline = time.time() + args.timeout
    while len(got) < total:
        remain = deadline - time.time()
        if remain <= 0:
            print('[test] 接收超时')
            break
        sock.settimeout(min(3.0, remain))
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            break
        except OSError as exc:
            print(f'[test] 接收异常: {exc!r}')
            break
        if not chunk:
            print('[test] 对端关闭')
            break
        got += chunk

    sock.close()
    th.join(timeout=1.0)

    open('/tmp/big_out.bin', 'wb').write(bytes(got))
    print(f'[test] sent = {total}, recv = {len(got)}, 差额 = {total - len(got)}')
    print(f'[test] 回包已写入 /tmp/big_out.bin')

    # 逐字节比对，报告第一个差异
    n = min(len(got), total)
    first_diff = -1
    for i in range(n):
        if got[i] != payload[i]:
            first_diff = i
            break

    ok = (len(got) == total) and (first_diff < 0)
    if first_diff >= 0:
        lo = max(0, first_diff - 32)
        print(f'[test] ✗ 第一个差异位于偏移 {first_diff}')
        print(f'        期望: {payload[lo:first_diff + 64]!r}')
        print(f'        实际: {bytes(got[lo:first_diff + 64])!r}')
    elif len(got) != total:
        print(f'[test] ✗ 长度不符（收到 {len(got)} / 应得 {total}）')
        if len(got) > total:
            print(f'        多出的前 64 字节: {bytes(got[total:total+64])!r}')
            print(f'        多出的总长度: {len(got) - total}')
    else:
        print('[test] ✓ 收发完全一致')

    for exc in send_err:
        print(f'[test] 发送侧错误: {exc!r}')
        ok = False

    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())