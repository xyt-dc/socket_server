#!/usr/bin/env python3
"""
handle_write 触发场景测试（覆盖 bigfile_echo 的功能）

目的
    用 strace 区分两种 send 来源，并给出可复现的「handle_write 被调用」场景。

判据（关键，别记错）
    EpollServer::handle_read 的 recv 缓冲是 KReadBuf = 4096，
    所以 flush() 里 ::send 的长度永远 <= 4096。
    => strace 里任何 sendto 请求长度 > 4096，一定来自 handle_write()
       （flush 那条路径永远发不出比 buf 更大的包）

wbuf 水位公式（也是精确的）
    wbuf 字节数 = 累计 recv 成功字节 - 累计 send 成功字节
    因为只有 send 成功返回后才调用 wbuf_retrieve()，
    所以这两个累计值之差就是当前躺在 _wbuf 里的字节数。

场景表
    ┌────────────┬──────────────────────────────┬──────────────────────────────┐
    │ --mode     │ 客户端行为                    │ 预期                          │
    ├────────────┼──────────────────────────────┼──────────────────────────────┤
    │ baseline   │ 立刻全速读                    │ handle_write = 0 次，wbuf≈0  │
    │ slow-read  │ rcvbuf=8K，每 1ms 只读 2KB    │ handle_write 多次，len>4096  │
    │ stall      │ 发完先憋 N 秒不读，再全速读    │ 一次吐 N MB（内存炸弹现场）   │
    │ never-read │ 发完一直不读，hold 住          │ wbuf 膨胀到全量，永不回落 ❌  │
    └────────────┴──────────────────────────────┴──────────────────────────────┘

用法
    # 自动起服务 + strace（推荐，一条命令出结论）
    python3 test/handle_write_trigger.py --mode=baseline
    python3 test/handle_write_trigger.py --mode=slow-read
    python3 test/handle_write_trigger.py --mode=stall  --stall=8
    python3 test/handle_write_trigger.py --mode=never-read --hold=5

    # 用 hamlet 当载荷
    python3 test/handle_write_trigger.py --mode=stall --file=test/hamlet --repeat=42

    # 自己起服务 / 想用 gdb 挂上去（--hold 会在峰值处停下并打印 pid）
    ./build/bench_server --server=epoll --port=6600 &
    python3 test/handle_write_trigger.py --mode=stall --no-spawn --port=6600 --hold=30

gdb 调试建议
    b Epoll_server.hpp:<handle_write 里的 send 那行>
    然后在 --hold 期间 attach 上去，看 conn_ptr->_wbuf.size()
"""
import argparse
import hashlib
import os
import re
import socket
import subprocess
import sys
import threading
import time

KREAD = 4096                     # 与 EpollServer::KReadBuf 保持一致
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(ROOT, 'build', 'bench_server')

# strace 输出（-s 0 => 字符串恒为 ""，配合 \s* 容忍自动折行）
EV = re.compile(
    r'(?:(\d+)\s+)?'                       # pid
    r'(?:(\d+\.\d+)\s+)?'                  # -tt 时间戳
    r'(sendto|recvfrom)\('
    r'\s*(\d+),\s*""[^,]*,\s*(\d+),\s*[^,]+,\s*[^,]+,\s*[^)]*\)'
    r'\s*=\s*(-?\d+)',
    re.S)


def fmt(n):
    return f'{n:,}'


def make_data(nbytes):
    """每 64 字节 = 8 位大端序号 + '.'，任何错位/丢失都能定位。"""
    out = bytearray()
    i = 0
    while len(out) < nbytes:
        out += (b'%08d' % i).ljust(64, b'.')
        i += 1
    return bytes(out[:nbytes])


# ---------------------------------------------------------------- 服务端进程

def wait_port(port, timeout=8.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def port_busy(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.3):
            return True
    except OSError:
        return False


def kill_port_owner(port):
    """杀掉占着这个端口的 bench_server 野进程（上一次没清干净的）。"""
    try:
        out = subprocess.check_output(
            ['pgrep', '-f', f'bench_server.*--port={port}'], text=True)
    except subprocess.CalledProcessError:
        return False
    killed = False
    for line in out.split():
        try:
            os.kill(int(line), 9)          # 必须 SIGKILL：SIGTERM 会被优雅停机吞掉
            killed = True
        except OSError:
            pass
    if killed:
        time.sleep(0.4)
    return killed


class Server:
    """直接跑 bench_server，再用 strace -p 挂上去。

    为什么不 `strace ./bench_server`：
        strace 收到 SIGTERM 时默认 **detach 而不是杀 tracee**，
        被跟踪的 bench_server 就变成孤儿野进程继续占端口。
        下一次测试新起的进程 EADDRINUSE 秒退，strace 文件是空的，
        客户端却连上了野进程 —— 测试「通过」，但什么都没测到。
    所以这里让 bench_server 归我们所有，strace 只是旁观者。
    """

    def __init__(self, port, strace_path):
        self.port = port
        self.strace = strace_path
        self.srv = None
        self.tracer = None
        self.pid = None

    def start(self):
        if port_busy(self.port) and not kill_port_owner(self.port):
            raise RuntimeError(f':{self.port} 已被别的进程占用，换个 --port')
        if os.path.exists(self.strace):
            os.unlink(self.strace)

        self.srv = subprocess.Popen(
            [BENCH, '--server=epoll', f'--port={self.port}'],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.pid = self.srv.pid
        if not wait_port(self.port):
            self.stop()
            raise RuntimeError(f'bench_server 没在 :{self.port} 起来 '
                               f'（从 {BENCH} 编译过了吗？）')

        self.tracer = subprocess.Popen(
            ['strace', '-f', '-s', '0', '-tt', '-q',
             '-e', 'trace=sendto,recvfrom',
             '-o', self.strace, '-p', str(self.pid)],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.6)                    # attach 会短暂暂停 tracee，等它恢复
        return self

    def stop(self):
        # 先收 strace：SIGTERM 让它 flush 落盘后退出（tracee 不会被杀）
        if self.tracer and self.tracer.poll() is None:
            self.tracer.terminate()
            try:
                self.tracer.wait(3)
            except subprocess.TimeoutExpired:
                self.tracer.kill()
                self.tracer.wait()
        # 再自己动手杀 bench_server —— 它是我们的孩子，杀得干净
        if self.srv and self.srv.poll() is None:
            self.srv.kill()                # SIGKILL，别指望优雅停机
            try:
                self.srv.wait(3)
            except subprocess.TimeoutExpired:
                pass
        kill_port_owner(self.port)         # 兜底：确保端口被释放


# ---------------------------------------------------------------- strace 分析

def parse_strace(path):
    try:
        txt = open(path, errors='replace').read()
    except OSError:
        return []
    evs = []
    for m in EV.finditer(txt):
        evs.append({
            'ts': float(m.group(2)) if m.group(2) else None,
            'kind': m.group(3),
            'req': int(m.group(5)),
            'ret': int(m.group(6)),
        })
    return evs


def analyze(evs):
    """返回统计 + wbuf 时间线。"""
    rcv_ok = snd_ok = 0
    hw = []                       # handle_write 的 send（req > KREAD）
    flush_snd = 0
    eagain = 0
    wbuf_peak = 0
    wbuf_peak_at = None
    timeline = []
    t0 = next((e['ts'] for e in evs if e['ts']), None)

    for e in evs:
        if e['kind'] == 'recvfrom':
            if e['ret'] > 0:
                rcv_ok += e['ret']
        else:
            if e['ret'] > 0:
                snd_ok += e['ret']
                if e['req'] > KREAD:
                    hw.append(e)
                else:
                    flush_snd += 1
            elif e['ret'] < 0:
                eagain += 1

        now = rcv_ok - snd_ok
        if now > wbuf_peak:
            wbuf_peak = now
            wbuf_peak_at = e
        timeline.append((e, rcv_ok, snd_ok, now))

    return {
        'rcv_ok': rcv_ok,
        'snd_ok': snd_ok,
        'eagain': eagain,
        'hw': hw,
        'flush_snd': flush_snd,
        'wbuf_peak': wbuf_peak,
        'wbuf_peak_at': wbuf_peak_at,
        'timeline': timeline,
        't0': t0,
    }


def print_report(res, mode, total):
    hw = res['hw']
    print()
    print('=' * 72)
    print('strace 分析')
    print('=' * 72)
    print(f"  recv 成功累计        : {fmt(res['rcv_ok'])} B")
    print(f"  send 成功累计        : {fmt(res['snd_ok'])} B")
    print(f"  send EAGAIN 次数     : {res['eagain']}")
    print(f"  flush 直发次数(≤4096): {res['flush_snd']}")
    print(f"  handle_write 次数    : {len(hw)}   ← req > 4096 的 sendto")
    if hw:
        print(f"    最大单次请求       : {fmt(max(e['req'] for e in hw))} B")
        print(f"    前 6 次请求长度    : {[fmt(e['req']) for e in hw[:6]]}")
        print(f"    前 6 次实际发出    : "
              f"{[fmt(e['ret']) for e in hw[:6]]}")
    print(f"  wbuf 峰值（推算）    : {fmt(res['wbuf_peak'])} B"
          f"  ({res['wbuf_peak'] / 1024 / 1024:.2f} MB)")

    # 关键事件上下文
    if hw:
        tl = res['timeline']
        idx = next(i for i, row in enumerate(tl) if row[0] is hw[0])
        print()
        print('  第一次 handle_write 之前的 6 条 / 之后 6 条：')
        for i in range(max(0, idx - 6), min(len(tl), idx + 6)):
            e, r, s, w = tl[i]
            tag = 'R' if e['kind'] == 'recvfrom' else 'S'
            mark = ' <<< 首次 handle_write' if i == idx else ''
            ret = 'EAGAIN' if e['ret'] < 0 else fmt(e['ret'])
            print(f'    {tag} req={fmt(e["req"]):>10}  ret={ret:<10}'
                  f'  wbuf={fmt(w):>10}{mark}')

    # 场景断言
    print()
    print('-' * 72)
    print('场景判定')
    print('-' * 72)
    ok = True
    if mode == 'baseline':
        if len(hw) == 0:
            print('  ✓ 符合预期：从没进过 handle_write（客户端一直在读，send 不撞墙）')
        else:
            print(f'  ✗ 不符预期：出现 {len(hw)} 次 handle_write')
            ok = False
    elif mode in ('slow-read', 'stall'):
        if hw:
            print(f'  ✓ 符合预期：handle_write 被调用 {len(hw)} 次，'
                  f'最大 {fmt(max(e["req"] for e in hw))} B')
        else:
            print('  ✗ 不符预期：一次都没进 handle_write —— 压得不够狠，'
                  '把 --rcvbuf 调小或加 --stall')
            ok = False
        if res['wbuf_peak'] > KREAD:
            print(f'  ✓ wbuf 真的堆积过：峰值 {fmt(res["wbuf_peak"])} B '
                  f'({res["wbuf_peak"] / 1024 / 1024:.2f} MB)')
        else:
            print(f'  ✗ wbuf 峰值只有 {fmt(res["wbuf_peak"])} B，'
                  f'没堆起来（这条路径没被压到）')
            ok = False
    elif mode == 'never-read':
        if res['wbuf_peak'] > KREAD * 16:
            print(f'  ⚠ 危险场景成立：客户端一次都没读，wbuf 仍停在 '
                  f'{fmt(res["wbuf_peak"])} B ({res["wbuf_peak"]/1024/1024:.2f} MB)')
            print('     这就是内存泄漏的形状 —— 连接活着多久，就占多久。')
            print('     加 kMaxWbufSize 上限后，这里应该变成：连接被 del_and_close。')
        else:
            print('  ✗ 没压出来，加大 --size 或 --hold')
            ok = False
    return ok


# ---------------------------------------------------------------- 客户端

def pause_hint(args, label):
    if args.hold <= 0:
        return
    print()
    print('  ' + '-' * 68)
    print(f'  [pause] {label}，现在停 {args.hold}s 方便你挂调试器')
    if args.server_pid:
        print(f'          服务端 pid = {args.server_pid}')
        print(f'          gdb -p {args.server_pid}')
        print(f"          (gdb) b Epoll_server.hpp:264      # handle_write 里的 send")
        print(f"          (gdb) p conn_ptr->_wbuf.size()")
    else:
        print(f'          pgrep -f "bench_server.*--port={args.port}" 拿 pid')
    print('  ' + '-' * 68)
    print(flush=True)


def read_loop(sock, got, total, deadline, chunk, sleep):
    while len(got) < total:
        remain = deadline - time.time()
        if remain <= 0:
            print('[client] 接收超时')
            return
        sock.settimeout(min(5.0, remain))
        want = min(chunk, total - len(got))
        try:
            b = sock.recv(want if want > 0 else 1)
        except socket.timeout:
            # ★ 关键：超时不 break。break 会把「读得慢」误判成「卡死」，
            #   之前 0.28x 假阴性就是这么来的。
            continue
        except OSError as exc:
            print(f'[client] 接收异常 {exc!r}')
            return
        if not b:
            print('[client] 对端关闭')
            return
        got += b
        if sleep:
            time.sleep(sleep)


def run_client(args, payload):
    total = len(payload)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if args.rcvbuf:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.rcvbuf)
        print(f'[client] SO_RCVBUF = '
              f'{sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)}')
    sock.settimeout(10.0)
    sock.connect(('127.0.0.1', args.port))
    print(f'[client] connected -> 127.0.0.1:{args.port}')

    send_err = []

    def sender():
        try:
            sock.sendall(payload)
            print('[client] 发送完成')
        except Exception as exc:                     # noqa: BLE001
            send_err.append(exc)
            print(f'[client] 发送异常 {exc!r}')

    th = threading.Thread(target=sender, daemon=True)
    th.start()

    got = bytearray()
    deadline = time.time() + args.timeout

    if args.mode == 'baseline':
        read_loop(sock, got, total, deadline, 65536, 0)

    elif args.mode == 'slow-read':
        print(f'[client] 慢读：每次 {args.chunk} B，间隔 {args.sleep*1000:.1f} ms')
        read_loop(sock, got, total, deadline, args.chunk, args.sleep)

    elif args.mode == 'stall':
        print(f'[client] 静默 {args.stall}s 不读 —— 逼服务端 send 撞 EAGAIN')
        pause_hint(args, f'wbuf 正在膨胀（静默 {args.stall}s 中）')
        time.sleep(args.stall)
        print('[client] 开始读 —— EPOLLOUT 该亮了')
        read_loop(sock, got, total, deadline, 65536, 0)

    elif args.mode == 'never-read':
        print(f'[client] 永不读，hold {args.hold}s（服务端 wbuf 只进不出）')
        pause_hint(args, 'wbuf 只进不出的现场')
        th.join(timeout=min(args.hold, 10))
        time.sleep(args.hold)

    th.join(timeout=5)
    try:
        sock.close()
    except OSError:
        pass

    print(f'[client] sent = {fmt(total)}  recv = {fmt(len(got))}  '
          f'差 = {fmt(total - len(got))}')
    return got, payload, send_err


def verify(got, payload, send_err):
    n = min(len(got), len(payload))
    first = -1
    for i in range(n):
        if got[i] != payload[i]:
            first = i
            break
    ok = (len(got) == len(payload)) and first < 0 and not send_err
    if first >= 0:
        lo = max(0, first - 32)
        print(f'[verify] ✗ 首个差异 @ {first}')
        print(f'         期望 {payload[lo:first+64]!r}')
        print(f'         实际 {bytes(got[lo:first+64])!r}')
    elif len(got) != len(payload):
        print(f'[verify] ✗ 长度不符 {len(got)} / {len(payload)}')
    elif not send_err:
        print('[verify] ✓ 字节完全一致')
    for exc in send_err:
        print(f'[verify] ✗ 发送侧错误 {exc!r}')
    if got:
        print(f'[verify] 回包 sha256 = '
              f'{hashlib.sha256(bytes(got)).hexdigest()[:16]}…')
    return ok


def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='handle_write 触发场景测试',
        epilog='场景：baseline / slow-read / stall / never-read')
    ap.add_argument('--mode', default='stall',
                    choices=['baseline', 'slow-read', 'stall', 'never-read'])
    ap.add_argument('--port', type=int, default=6600)
    ap.add_argument('--size', type=int, default=8 * 1024 * 1024)
    ap.add_argument('--file', default=None, help='载荷文件，缺省用生成数据')
    ap.add_argument('--repeat', type=int, default=1, help='载荷重复次数')
    ap.add_argument('--rcvbuf', type=int, default=0,
                    help='SO_RCVBUF，0=系统默认；slow-read 建议 8192')
    ap.add_argument('--chunk', type=int, default=2048, help='slow-read 每次读多少')
    ap.add_argument('--sleep', type=float, default=0.001,
                    help='slow-read 每次读后睡多久（秒）')
    ap.add_argument('--stall', type=float, default=8.0,
                    help='stall 模式静默多少秒')
    ap.add_argument('--hold', type=float, default=0.0,
                    help='在关键点停下来方便 gdb attach（秒）')
    ap.add_argument('--timeout', type=float, default=240.0,
                    help='客户端接收总超时（秒）')
    ap.add_argument('--no-spawn', action='store_true',
                    help='不自动起服务（自己起 / 自己 attach）')
    ap.add_argument('--strace', default='/tmp/handle_write.strace')
    args = ap.parse_args()

    if args.file:
        payload = open(args.file, 'rb').read() * args.repeat
    else:
        payload = make_data(args.size) * args.repeat
    print(f'[test] mode = {args.mode}   payload = {fmt(len(payload))} B '
          f'({len(payload)/1024/1024:.2f} MB)')

    srv = None
    args.server_pid = None
    try:
        if not args.no_spawn:
            if not os.path.exists(BENCH):
                print(f'[test] 找不到 {BENCH}，先 cmake --build build --target bench_server')
                return 2
            srv = Server(args.port, args.strace).start()
            args.server_pid = srv.pid
            print(f'[test] 服务已起（strace -> {args.strace}）'
                  + (f' pid={srv.pid}' if srv.pid else ''))
        else:
            print(f'[test] --no-spawn：假定 :{args.port} 上已有服务')

        got, payload, send_err = run_client(args, payload)
        echo_ok = verify(got, payload, send_err)
    finally:
        if srv:
            srv.stop()

    if args.no_spawn:
        print('\n[test] --no-spawn 模式不做 strace 分析。')
        print('       想要判据，自己起服务时加：')
        print(f'       strace -f -s0 -tt -e trace=sendto,recvfrom -o {args.strace} \\')
        print(f'         ./build/bench_server --server=epoll --port={args.port}')
        return 0 if echo_ok else 1

    time.sleep(0.3)
    evs = parse_strace(args.strace)
    if not evs:
        print(f'\n[test] {args.strace} 里没解析到事件，检查 strace 是否可用')
        return 2
    res = analyze(evs)
    ok = print_report(res, args.mode, len(payload))
    print()
    print(f'  回显一致性: {"✓" if echo_ok else "✗"}   '
          f'场景判定: {"✓" if ok else "✗"}')
    return 0 if bool(ok) else 1


if __name__ == '__main__':
    sys.exit(main())