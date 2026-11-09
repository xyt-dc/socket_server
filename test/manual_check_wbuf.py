#!/usr/bin/env python3
"""
验证 EpollServer::handle_write 是否重复发送 wbuf 数据。

原理：
  客户端发 N 字节后**故意不读**，让服务端 kernel 发送缓冲区灌满。
  服务端 send 返回 EAGAIN -> 剩余进 wbuf + 注册 EPOLLOUT。
  客户端开始读，此时 EPOLLOUT 反复触发 -> handle_write 被调用。
  如果 handle_write 从不消费 wbuf（缺 wbuf_retrieve），
  它每轮都会把整个 wbuf 重新 send 一遍 -> 客户端收到的字节数 >> N。

用法: python3 test/manual_check_wbuf.py [port] [MB]
"""
import socket
import sys
import threading
import time

port = int(sys.argv[1]) if len(sys.argv) > 1 else 6420
mb = int(sys.argv[2]) if len(sys.argv) > 2 else 20
total = mb * 1024 * 1024

s = socket.create_connection(("127.0.0.1", port))
s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)  # 小接收窗口，尽快逼出 EAGAIN


def sender():
    try:
        s.sendall(b"A" * total)
        print(f"[sender] 已发完 {total} 字节")
    except Exception as e:
        print(f"[sender] 异常: {e}")


t = threading.Thread(target=sender, daemon=True)
t.start()

print(f"[main] 客户端故意不读 {6} 秒，逼服务端发送缓冲区灌满 ...")
time.sleep(6)

print("[main] 开始读，观察是否收到超过预期")
got = 0
deadline = time.time() + 25
s.settimeout(3)
while time.time() < deadline:
    try:
        d = s.recv(1 << 20)
    except socket.timeout:
        break
    except Exception as e:
        print(f"[main] recv 异常: {e}")
        break
    if not d:
        break
    got += len(d)
    if got > total * 3:
        print(f"[main] 已收到 {got} 字节，超过 3 倍，提前停止")
        break

print()
print("=" * 60)
print(f"客户端发送: {total} 字节")
print(f"客户端收到: {got} 字节")
print(f"放大倍数  : {got / total:.2f}x")
if got > total * 1.05:
    print("结论: ✗ wbuf 被重复发送 —— handle_write 没有消费 wbuf")
elif got == total:
    print("结论: ✓ 收发平衡")
else:
    print(f"结论: ? 收到 {(total - got)/total:.1%} 缺失")
print("=" * 60)

s.close()