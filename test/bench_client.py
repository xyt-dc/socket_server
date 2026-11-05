#!/usr/bin/env python3
"""
=============================================================================
 bench_client.py  ——  echo 压测客户端
=============================================================================

【为什么用 asyncio 而不是每连接一个线程】
  要测 200 个并发连接，用线程就是 200 个 OS 线程，
  客户端的线程调度开销会盖过服务端的差异。
  asyncio 用单线程事件循环，2000 个连接也是 1 个线程，
  这样测出来的数字才归因于服务端。

【四种模式】
  smoke      每条连接发几条消息就断，验证功能正确
  latency    每条连接串行 ping-pong，测 RTT 分布（p50/p90/p99/max）
  throughput 每条连接不等回复连续发，测服务端吞吐上限
  hold       连上就不发数据，测服务端能扛多少空闲连接

【核心判据】
  mismatch > 0  → 数据错了（echo 回来的和发出去的不一样）
  errors   > 0  → 连接被拒/被断（背压生效或崩溃）
  refused       → 建连就被拒（listen backlog 或 fd 耗尽）

用法
  ./bench_client.py --port=6201 --mode=latency --conns=50 --msgs=100
  ./bench_client.py --port=6201 --mode=throughput --conns=20 --msgs=500
  ./bench_client.py --port=6201 --mode=hold --conns=200 --hold=5
  ./bench_client.py --port=6201 --json      # 机器可读，方便脚本比对

退出码
  0 = 全部正确     1 = 有 mismatch/error     2 = 参数或环境问题
=============================================================================
"""

import argparse
import asyncio
import json
import socket
import statistics
import sys
import time


class Stats:
    def __init__(self):
        self.latencies = []      # 秒
        self.sent = 0
        self.recv = 0
        self.mismatch = 0
        self.errors = 0
        self.refused = 0
        self.conn_ok = 0
        self.conn_fail = 0
        # 服务端「一个字节都没回就关了连接」的次数
        # 区别很重要：
        #   eof_early  = accept 后直接 close（背压拒绝 / worker 没被调度到就关了）
        #   mismatch   = 回数据了，但内容不对（真正的数据损坏）
        #   partial    = 回了半条消息就断了（send 部分写没处理 → 最严重的缺陷）
        self.eof_early = 0
        self.partial = 0
        self.err_detail = []     # 最多留 5 条，避免刷屏

    def pct(self, p):
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        # 最近秩法，避免插值把 p99 算低
        idx = min(len(s) - 1, int(round(p / 100.0 * len(s) + 0.5)) - 1)
        return s[max(0, idx)]

    def summary(self):
        lat = self.latencies
        return {
            "conn_ok": self.conn_ok,
            "conn_fail": self.conn_fail,
            "refused": self.refused,
            "sent": self.sent,
            "recv": self.recv,
            "mismatch": self.mismatch,
            "errors": self.errors,
            "eof_early": self.eof_early,
            "partial": self.partial,
            "lat_count": len(lat),
            "p50_ms": round(self.pct(50) * 1000, 3),
            "p90_ms": round(self.pct(90) * 1000, 3),
            "p99_ms": round(self.pct(99) * 1000, 3),
            "max_ms": round((max(lat) if lat else 0.0) * 1000, 3),
            "mean_ms": round((statistics.fmean(lat) if lat else 0.0) * 1000, 3),
        }


def make_payload(conn_id: int, seq: int, size: int) -> bytes:
    """
    生成的 payload 必须能唯一识别，否则 mismatch 检测会失效。
    格式： "c{conn}:{seq}:" + 填充字符，总长 = size。
    """
    head = f"c{conn_id}:{seq}:".encode()
    if len(head) >= size:
        return head[:size]
    return head + b"x" * (size - len(head))


async def read_exact_or_classify(r, n: int, st: Stats, timeout: float = 5.0):
    """
    读满 n 字节，把失败原因归类后返回 None，而不是抛异常。

    为什么必须分类：这三种情况的诊断结论完全不同
      服务端直接 close         → IncompleteReadError(0 bytes)  → eof_early
      服务端发了半条就断       → IncompleteReadError(k bytes)  → partial ★最严重
      服务端不回但连接还在     → TimeoutError                 → errors
    """
    try:
        return await asyncio.wait_for(r.readexactly(n), timeout)
    except asyncio.TimeoutError:
        st.errors += 1
        return None
    except asyncio.IncompleteReadError as e:
        if len(e.partial) == 0:
            st.eof_early += 1
        else:
            # 收到了部分数据，说明服务端发出去了但没发完
            # 这正是「send 遇 EAGAIN 直接 return」缺陷的表现
            st.partial += 1
            st.err_detail.append(
                f"partial read: got {len(e.partial)}/{n} bytes"
            )
        return None
    except (ConnectionResetError, BrokenPipeError):
        st.errors += 1
        return None


async def connect(host, port, st: Stats, timeout=5.0):
    """建连；失败计入 refused/conn_fail，返回 None 而不是抛异常。"""
    try:
        r, w = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        st.conn_ok += 1
        return r, w
    except (ConnectionRefusedError, OSError) as e:
        # 背压生效时，accept 队列满 → 内核直接 RST → ECONNREFUSED
        if isinstance(e, ConnectionRefusedError):
            st.refused += 1
        else:
            st.conn_fail += 1
        return None


async def mode_smoke(args, st: Stats):
    """功能验证：每条连接发 3 条，逐条比对。"""
    async def one(cid):
        r, w = await connect(args.host, args.port, st)
        if w is None:
            return
        try:
            for seq in range(3):
                payload = make_payload(cid, seq, args.size)
                w.write(payload)
                await w.drain()
                st.sent += 1
                echoed = await read_exact_or_classify(r, len(payload), st)
                if echoed is None:
                    return
                st.recv += 1
                if echoed != payload:
                    st.mismatch += 1
        finally:
            w.close()
            try:
                await w.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    await asyncio.gather(*(one(i) for i in range(args.conns)))


async def mode_latency(args, st: Stats):
    """串行 ping-pong：每条连接同一时刻只有一个请求在飞，RTT 才有意义。"""
    async def one(cid):
        r, w = await connect(args.host, args.port, st)
        if w is None:
            return
        try:
            for seq in range(args.msgs):
                payload = make_payload(cid, seq, args.size)
                t0 = time.perf_counter()
                w.write(payload)
                await w.drain()
                st.sent += 1
                echoed = await read_exact_or_classify(r, len(payload), st)
                if echoed is None:
                    return
                st.latencies.append(time.perf_counter() - t0)
                st.recv += 1
                if echoed != payload:
                    st.mismatch += 1
        finally:
            w.close()
            try:
                await w.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    await asyncio.gather(*(one(i) for i in range(args.conns)))


async def mode_throughput(args, st: Stats):
    """
    流水线模式：每条连接连续写 msgs 条，然后一次性读回。
    这样测的是服务端「能吞多少」，不掺入 RTT 等待。
    """
    async def one(cid):
        r, w = await connect(args.host, args.port, st)
        if w is None:
            return
        total = args.msgs * args.size
        payloads = [make_payload(cid, seq, args.size) for seq in range(args.msgs)]
        try:
            t0 = time.perf_counter()
            for p in payloads:
                w.write(p)
            await w.drain()
            st.sent += args.msgs

            # 累计读完 total 字节
            got = bytearray()
            while len(got) < total:
                try:
                    chunk = await asyncio.wait_for(r.read(total - len(got)), 30.0)
                except asyncio.TimeoutError:
                    st.errors += 1
                    break
                if not chunk:
                    # 服务端提前关闭（例如背压丢弃了这条连接）
                    st.errors += 1
                    break
                got.extend(chunk)
            st.recv += len(got) // args.size
            st.latencies.append(time.perf_counter() - t0)

            # 逐条比对（按 size 切分）
            expected = b"".join(payloads)
            if bytes(got) != expected:
                # 允许长度不一致时只记一次，避免刷屏
                st.mismatch += 1
        finally:
            w.close()
            try:
                await w.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    await asyncio.gather(*(one(i) for i in range(args.conns)))


async def mode_hold(args, st: Stats):
    """只建连不发数据：测服务端能同时维持多少空闲连接。"""
    writers = []

    async def one(cid):
        r, w = await connect(args.host, args.port, st)
        if w is None:
            return
        writers.append(w)
        try:
            await asyncio.sleep(args.hold)
        finally:
            w.close()
            try:
                await w.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    await asyncio.gather(*(one(i) for i in range(args.conns)))


MODES = {
    "smoke": mode_smoke,
    "latency": mode_latency,
    "throughput": mode_throughput,
    "hold": mode_hold,
}


def main():
    ap = argparse.ArgumentParser(description="echo server benchmark client")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6200)
    ap.add_argument("--mode", choices=sorted(MODES), default="latency")
    ap.add_argument("--conns", type=int, default=20, help="并发连接数")
    ap.add_argument("--msgs", type=int, default=50, help="每条连接的消息数")
    ap.add_argument("--size", type=int, default=64, help="单条消息字节数")
    ap.add_argument("--hold", type=float, default=3.0, help="hold 模式保持秒数")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    args = ap.parse_args()

    if args.conns <= 0 or args.msgs <= 0 or args.size <= 0:
        print("conns/msgs/size must be > 0", file=sys.stderr)
        return 2

    st = Stats()
    t0 = time.perf_counter()
    try:
        asyncio.run(MODES[args.mode](args, st))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 2
    elapsed = time.perf_counter() - t0

    out = st.summary()
    out["mode"] = args.mode
    out["conns_target"] = args.conns
    out["payload_size"] = args.size
    out["elapsed_s"] = round(elapsed, 3)
    # 吞吐按「实际收到的字节」算，避免发出去但没回来的虚高
    out["msg_per_s"] = round(out["recv"] / elapsed, 1) if elapsed > 0 else 0.0
    out["mib_per_s"] = round(out["recv"] * args.size / elapsed / (1024 * 1024), 2) if elapsed > 0 else 0.0

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"mode={out['mode']}  elapsed={out['elapsed_s']}s")
        print(f"connections   ok={out['conn_ok']}  fail={out['conn_fail']}  refused={out['refused']}")
        print(f"messages      sent={out['sent']}  recv={out['recv']}  mismatch={out['mismatch']}  errors={out['errors']}")
        if out["eof_early"] or out["partial"]:
            print(f"failures      eof_early={out['eof_early']}  partial={out['partial']}")
        print(f"throughput    {out['msg_per_s']} msg/s   {out['mib_per_s']} MiB/s")
        if out["lat_count"]:
            print(f"latency(ms)   p50={out['p50_ms']}  p90={out['p90_ms']}  "
                  f"p99={out['p99_ms']}  max={out['max_ms']}  mean={out['mean_ms']}")
        for d in st.err_detail[:5]:
            print(f"  · {d}")

    # partial 是「发出去了但没发完」，属于数据完整性缺陷，必须算失败
    bad = out["mismatch"] > 0 or out["errors"] > 0 or out["partial"] > 0
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())