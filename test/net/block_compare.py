#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
 block_compare.py —— 业务阻塞下  epoll(1 reactor + N worker)  vs  olpt(N reactor)
=============================================================================

【为什么需要这个脚本】
  纯 echo 业务是微秒级的，两个模型打出来的数字几乎一样，看不出区别。
  真正的分水岭是「业务里有一行阻塞代码」：

    EpollServer   业务跑在线程池的 worker 线程里
                  → reactor 线程不受影响，accept / recv / close 全部照常

    OLPT_Server   两种形态，由 --pool-per-sub 决定：
                   0 = 业务跑在【从 reactor 自己的线程】里
                       → 那行 sleep 一执行，本 shard 的 epoll_wait 也停了
                         该 shard 上所有连接的 I/O 一起停摆
                   1 = 每个从 reactor 挂一个单线程业务池（默认）
                       → I/O 线程不再被业务占用，E4 的 2.18s 会掉到 0.19s

  四个实验，每个都同时跑两个模型，数字并排打印，结论自己看。

【实验清单】
  E1  基线          biz-delay=0     50 连接 × 100 条       ← 两者应该几乎一样
  E2  统一慢业务    biz-delay=2ms   16 连接 × 5 条         ← 排队效应，两者都接近
  E3  单慢请求      8 连接，其中 1 条请求 sleep 800ms
                    ← 看「队头阻塞」波及几条连接（4 个 worker/shard 时约 1/4）
  E4  慢任务占满    workers=1，1 条连接 sleep 2000ms
                    另一条连接灌 32MB，量 write 被阻塞多久
                    ← ★ olpt 用 --pool-per-sub=0 时是唯一能把两者拉开
                      数量级的实验（olpt ≈ 2.18s，epoll ≈ 0.18s）；
                      挂上业务池（默认 1）后两者持平，≈ 0.19s
  E5  线程数        慢业务 + 1000 空闲连接，读 /proc/PID/task
                    ← 验证两者都是「1 + N」个线程，与连接数无关

【用法】
  python3 test/net/block_compare.py
  python3 test/net/block_compare.py --workers=4
  python3 test/net/block_compare.py --only=E4
  python3 test/net/block_compare.py --only=E4 --pool-per-sub=0   # 复现 2.18s 对照
  python3 test/net/block_compare.py --no-build          # 跳过编译

【退出码】
  0 = 跑完（不判断好坏，这是观测脚本不是判据脚本）
  2 = 环境问题（bin 不存在 / 服务端起不来）
=============================================================================
"""

import argparse
import asyncio
import importlib.util
import os
import pathlib
import signal
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
BIN = ROOT / "build" / "bench_server"
PROBE_PATH = ROOT / "test" / "tlv" / "tlv_probe.py"


def load_probe():
    spec = importlib.util.spec_from_file_location("tlv_probe", PROBE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


probe = load_probe()


# =============================================================================
#  服务端句柄
# =============================================================================
class Server:
    """拉起一个 bench_server，退出时确保进程被回收。

    注意：不用「尝试连端口」来判断就绪 —— 那会额外消耗一次 accept，
    白白占掉一个 fd 号，把 E3 里 fd % N 的分片预测打乱。
    改成盯日志里的 "socket success"。
    """

    def __init__(self, model: str, port: int, workers: int = 4,
                 biz_delay_ms: int = 0, queue_cap: int = 200000,
                 pool_per_sub: int = 1):
        self.model = model
        self.port = port
        self.workers = workers
        self.biz_delay_ms = biz_delay_ms
        self.queue_cap = queue_cap
        self.pool_per_sub = pool_per_sub
        self.proc = None
        self.log_path = f"/tmp/block_compare_{model}_{port}.log"

    def __enter__(self):
        args = [str(BIN),
                f"--server={self.model}",
                f"--port={self.port}",
                f"--workers={self.workers}",
                f"--queue-cap={self.queue_cap}"]
        if self.biz_delay_ms:
            args.append(f"--biz-delay={self.biz_delay_ms}")
        # 只有 olpt 认这个参数；传 0 就是「业务在 I/O 线程里跑」的对照组
        if self.model == "olpt":
            args.append(f"--pool-per-sub={self.pool_per_sub}")

        self._log = open(self.log_path, "w")
        self.proc = subprocess.Popen(args, stdout=self._log,
                                     stderr=subprocess.STDOUT)

        deadline = time.time() + 8.0
        while time.time() < deadline:
            if self.proc.poll() is not None:
                self._log.flush()
                raise RuntimeError(
                    f"{self.model} 启动失败（exit={self.proc.returncode}），"
                    f"日志见 {self.log_path}")
            try:
                if "socket success" in pathlib.Path(self.log_path).read_text():
                    # 再等 50ms 让 listen 真正生效
                    time.sleep(0.05)
                    return self
            except FileNotFoundError:
                pass
            time.sleep(0.02)

        self._kill()
        raise RuntimeError(f"{self.model} 8s 内未就绪，日志见 {self.log_path}")

    def __exit__(self, *exc):
        self._kill()
        return False

    def _kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        try:
            self._log.close()
        except Exception:
            pass

    def threads(self) -> int:
        try:
            return len(os.listdir(f"/proc/{self.proc.pid}/task"))
        except OSError:
            return -1

    def fds(self) -> int:
        try:
            return len(os.listdir(f"/proc/{self.proc.pid}/fd"))
        except OSError:
            return -1


# =============================================================================
#  客户端原语
# =============================================================================
async def read_one_frame(reader, dec, timeout=30.0):
    """从流里取出一个完整帧的 body。"""
    while True:
        got = dec.next()
        if got is not None:
            _hdr, body = got
            return body
        chunk = await asyncio.wait_for(reader.read(65536), timeout)
        if not chunk:
            raise ConnectionError("对端关闭")
        dec.feed(chunk)


def make_body(size: int, tag: str = "c") -> bytes:
    """生成 size 字节、内容可辨识的 body。"""
    head = tag.encode()
    if len(head) >= size:
        return head[:size]
    return head + b"x" * (size - len(head))


async def one_pingpong(port, n_msgs, size, slow_first_ms=0):
    """一条连接：串行 ping-pong n_msgs 次，返回每次的 RTT（秒）。"""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    dec = probe.Decoder()
    lat = []
    try:
        for i in range(n_msgs):
            if slow_first_ms and i == 0:
                body = f"SLOW:{slow_first_ms}:".encode()
            else:
                body = make_body(size)
            t0 = time.perf_counter()
            writer.write(probe.pack(probe.Cmd.ECHO_REQ, body, seq=i))
            await writer.drain()
            echoed = await read_one_frame(reader, dec)
            if echoed != body:
                raise RuntimeError("echo 内容不一致（协议层有 bug）")
            lat.append(time.perf_counter() - t0)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    return lat


def pct(sorted_list, q):
    if not sorted_list:
        return 0.0
    i = min(len(sorted_list) - 1, int(q / 100.0 * len(sorted_list)))
    return sorted_list[i] * 1000.0


def summarize(lat):
    s = sorted(lat)
    if not s:
        return dict(n=0, p50=0.0, p95=0.0, mx=0.0, mean=0.0)
    return dict(n=len(s),
                p50=pct(s, 50), p95=pct(s, 95),
                mx=s[-1] * 1000.0,
                mean=sum(s) / len(s) * 1000.0)


# =============================================================================
#  实验
# =============================================================================
async def exp1_baseline(port):
    """E1：无阻塞基线。50 连接 × 100 条，全部串行 ping-pong。"""
    tasks = [one_pingpong(port, 100, 64) for _ in range(50)]
    t0 = time.perf_counter()
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - t0
    lat = [x for r in results for x in r]
    st = summarize(lat)
    st["qps"] = len(lat) / elapsed
    return st


async def exp2_slow_biz(port, workers, msgs=5, delay_ms=2):
    """E2：所有业务统一 sleep。连接数 = 4×worker，必然排队。"""
    conns = workers * 4
    tasks = [one_pingpong(port, msgs, 64) for _ in range(conns)]
    results = await asyncio.gather(*tasks)
    lat = [x for r in results for x in r]
    st = summarize(lat)
    st["conns"] = conns
    # 稳态下每条业务线程上有 conns/workers 条连接在轮转，
    # 所以单条请求要等前面那一圈的 delay 走完：
    #     p50 ≈ (conns / workers) × delay
    st["expected_p50"] = (conns / workers) * delay_ms
    return st


async def exp3_head_of_line(port, conns=8, slow_ms=800):
    """E3：并发 8 条请求，其中第 1 条是 800ms 慢请求。

    先【顺序】建立连接（保证 fd 分配顺序确定），再【同时】发请求。
    返回每条连接的 RTT，看慢请求波及了几条。
    """
    pairs = []
    for _ in range(conns):
        r, w = await asyncio.open_connection("127.0.0.1", port)
        pairs.append((r, w, probe.Decoder()))

    async def go(idx):
        r, w, dec = pairs[idx]
        body = (f"SLOW:{slow_ms}:".encode() if idx == 0
                else make_body(64, f"f{idx}"))
        t0 = time.perf_counter()
        w.write(probe.pack(probe.Cmd.ECHO_REQ, body, seq=idx))
        await w.drain()
        await read_one_frame(r, dec)
        return time.perf_counter() - t0

    try:
        lats = await asyncio.gather(*(go(i) for i in range(conns)))
    finally:
        for r, w, _ in pairs:
            w.close()
        for r, w, _ in pairs:
            try:
                await w.wait_closed()
            except Exception:
                pass

    slow_threshold = slow_ms / 1000.0 * 0.5      # 超过一半慢请求时间就算被拖累
    victims = [i for i, t in enumerate(lats) if i != 0 and t >= slow_threshold]
    return dict(lats_ms=[round(t * 1000, 1) for t in lats],
                victims=victims,
                fast_max_ms=round(max([t for i, t in enumerate(lats) if i != 0],
                                      default=0) * 1000, 1))


async def exp4_flood(port, slow_ms=2000, flood_mb=32, frm_size=4096):
    """E4：1 个 worker。连接 A 发慢请求占住唯一线程，连接 B 灌 32MB。

    量什么：B 把数据写完（drain 返回）要多久。
      epoll → reactor 一直在 recv，B 很快写完
      olpt  → sub 在 sleep，没人 recv，B 被 TCP 窗口卡住
    """
    # A：占住业务线程
    ra, wa = await asyncio.open_connection("127.0.0.1", port)
    wa.write(probe.pack(probe.Cmd.ECHO_REQ,
                        f"SLOW:{slow_ms}:".encode(), seq=0))
    await wa.drain()
    await asyncio.sleep(0.1)          # 确保 sleep 已经开始

    # B：灌数据
    rb, wb = await asyncio.open_connection("127.0.0.1", port)
    payload = make_body(frm_size)
    frame = probe.pack(probe.Cmd.ECHO_REQ, payload, seq=1)
    total = flood_mb * 1024 * 1024
    written = 0
    t0 = time.perf_counter()
    blocked = False
    try:
        while written < total:
            wb.write(frame)
            written += len(frame)
            try:
                await asyncio.wait_for(wb.drain(), timeout=slow_ms / 1000.0 + 1.0)
            except asyncio.TimeoutError:
                blocked = True
                break
    except Exception:
        blocked = True
    write_elapsed = time.perf_counter() - t0

    for w in (wa, wb):
        w.close()
    for w in (wa, wb):
        try:
            await w.wait_closed()
        except Exception:
            pass

    return dict(write_s=round(write_elapsed, 3),
                written_mb=round(written / 1024 / 1024, 1),
                blocked=blocked)


def exp5_threads(srv):
    """E5：读线程数 / fd 数（服务端已在 hold 状态下）。"""
    return dict(threads=srv.threads(), fds=srv.fds())


# =============================================================================
#  编排与打印
# =============================================================================
MODELS = ("epoll", "olpt")
PORT_BASE = {"epoll": 6501, "olpt": 6511}
# 由 main() 里的 --pool-per-sub 覆盖；E1/E2/E3/E5 用它，
# E4 固定跑「无池 / 挂池」两组对照，不读这个全局。
POOL_PER_SUB = 1


def run_exp1(workers):
    print("\n【E1】基线：无业务阻塞，50 连接 × 100 条")
    rows = {}
    for m in MODELS:
        with Server(m, PORT_BASE[m], workers, pool_per_sub=POOL_PER_SUB) as srv:
            rows[m] = asyncio.run(exp1_baseline(srv.port))
    print(f"  {'模型':<8}{'p50(ms)':>10}{'p95(ms)':>10}{'max(ms)':>10}{'QPS':>12}")
    for m in MODELS:
        r = rows[m]
        print(f"  {m:<8}{r['p50']:>10.2f}{r['p95']:>10.2f}"
              f"{r['mx']:>10.2f}{r['qps']:>12.0f}")
    print("  → 纯 echo 下两者应该非常接近。差异全在业务阻塞时才出现。")


def run_exp2(workers):
    print(f"\n【E2】统一慢业务 2ms：{workers * 4} 连接 × 5 条（必然排队）")
    rows = {}
    for m in MODELS:
        with Server(m, PORT_BASE[m], workers, biz_delay_ms=2,
                    pool_per_sub=POOL_PER_SUB) as srv:
            rows[m] = asyncio.run(exp2_slow_biz(srv.port, workers))
    print(f"  {'模型':<8}{'p50(ms)':>10}{'p95(ms)':>10}{'max(ms)':>10}"
          f"{'预期 p50':>12}")
    for m in MODELS:
        r = rows[m]
        print(f"  {m:<8}{r['p50']:>10.2f}{r['p95']:>10.2f}"
              f"{r['mx']:>10.2f}{r['expected_p50']:>12.1f}")
    print("  → 排队模型相同：p50 ≈ (连接数/线程数) × 2ms，两者都对得上。")
    print("    想拉开差距看 E4。")


def run_exp3(workers):
    print("\n【E3】队头阻塞：8 条并发请求，其中 1 条 sleep 800ms")
    for m in MODELS:
        with Server(m, PORT_BASE[m], workers, pool_per_sub=POOL_PER_SUB) as srv:
            r = asyncio.run(exp3_head_of_line(srv.port))
        victims = r["victims"]
        print(f"  {m:<6} 各连接 RTT(ms) = {r['lats_ms']}")
        print(f"         {'':<4} 被拖累的连接下标 = {victims} "
              f"({len(victims)}/7 条), 快请求最慢 {r['fast_max_ms']} ms")
    print(f"  → {workers} 个 shard、8 条连接 ⇒ 每个 shard 2 条连接，")
    print(f"    慢请求所在 shard 的【另外 1 条】必然被拖住，其余 shard 毫发无伤。")
    print("    受害数是 (conns/workers - 1)，不是随机分布的——受害面完全由分片规则决定。")
    print("    这也是阶段 4 要解决的核心问题：单连接慢请求 = 同 shard 全体 SLA 下降。")


def run_exp4():
    print("\n【E4】★ 关键对比：唯一线程被 2000ms 慢请求占住时，另一连接灌 32MB")
    print("         （workers=1，业务线程一律只有 1 个）")
    print("         olpt 跑两遍：pool-per-sub=0（业务在 I/O 线程）vs =1（挂业务池）")
    cases = [
        ("epoll",        "epoll", 1),
        ("olpt 无池",     "olpt",  0),
        ("olpt 挂池",     "olpt",  1),
    ]
    rows = {}
    for label, model, pps in cases:
        # 用不同端口，避免上一轮 TIME_WAIT 干扰
        port = PORT_BASE[model] + (0 if pps == 1 else 100)
        with Server(model, port, 1, pool_per_sub=pps) as srv:
            rows[label] = asyncio.run(exp4_flood(srv.port))
    print(f"  {'配置':<12}{'写完耗时(s)':>14}{'已写(MB)':>12}{'被卡住':>10}")
    for label, _m, _p in cases:
        r = rows[label]
        print(f"  {label:<12}{r['write_s']:>14.3f}{r['written_mb']:>12.1f}"
              f"{('是' if r['blocked'] else '否'):>10}")
    print("  → epoll：reactor 与业务是不同线程，recv 从不停 → 灌得动")
    print("    olpt 无池：sub 线程在 sleep，连 epoll_wait 都没跑 → TCP 窗口打满，")
    print("              耗时 ≈ SLOW 的毫秒数 / 1000，I/O 完全停摆")
    print("    olpt 挂池：业务搬到 shard 自己的 worker 线程，I/O 线程继续跑 epoll_wait")
    print("              → 耗时掉回 0.2s 量级。这就是「I/O 与业务分离」的收益，")
    print("              也是 nginx / muduo 全都这么做的原因。")


def run_exp5(workers):
    print("\n【E5】线程数 / fd 数：1000 条空闲连接 + 慢业务")
    for m in MODELS:
        # epoll：1 reactor + N worker；olpt：1 主 + N sub（+ 可选 N×pool worker）
        if m == "epoll" or POOL_PER_SUB <= 0:
            expect = f"1 + {workers} = {1 + workers}"
        else:
            expect = (f"1 主 + {workers} sub + {workers * POOL_PER_SUB} worker "
                      f"= {1 + workers + workers * POOL_PER_SUB}")
        with Server(m, PORT_BASE[m], workers, biz_delay_ms=1,
                    pool_per_sub=POOL_PER_SUB) as srv:
            async def hold():
                ws = []
                for _ in range(1000):
                    r, w = await asyncio.open_connection("127.0.0.1", srv.port)
                    ws.append(w)
                await asyncio.sleep(0.5)
                snap = exp5_threads(srv)
                for w in ws:
                    w.close()
                return snap
            snap = asyncio.run(hold())
        print(f"  {m:<6} threads={snap['threads']:>3}  fds={snap['fds']:>5}"
              f"   （期望 {expect}，与 1000 条连接无关）")


def main():
    ap = argparse.ArgumentParser(description="epoll vs olpt 业务阻塞对比")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--only", default="", help="只跑某个实验，如 E4")
    ap.add_argument("--pool-per-sub", type=int, default=1,
                    help="olpt 每个从 reactor 的业务线程数（0=不挂池，对照组）")
    ap.add_argument("--no-build", action="store_true")
    args = ap.parse_args()

    global POOL_PER_SUB
    POOL_PER_SUB = args.pool_per_sub

    if not args.no_build:
        print("编译 bench_server ...")
        r = subprocess.run(["cmake", "--build", str(ROOT / "build"),
                            "--target", "bench_server", "-j"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            sys.stderr.write(r.stdout + r.stderr)
            return 2

    if not BIN.exists():
        sys.stderr.write(f"找不到 {BIN}\n")
        return 2

    print("=" * 72)
    print(f" epoll(1 reactor + {args.workers} worker)  vs  "
          f"olpt({args.workers} sub reactor, pool-per-sub={POOL_PER_SUB})")
    print(f" CPU 核数 = {os.cpu_count()}")
    print("=" * 72)

    plan = {
        "E1": lambda: run_exp1(args.workers),
        "E2": lambda: run_exp2(args.workers),
        "E3": lambda: run_exp3(args.workers),
        "E4": run_exp4,
        "E5": lambda: run_exp5(args.workers),
    }
    keys = [args.only.upper()] if args.only else list(plan)
    for k in keys:
        if k not in plan:
            sys.stderr.write(f"未知实验 {k}\n")
            return 2
        plan[k]()

    print("\n" + "=" * 72)
    print(" 结论看 E4：业务与 I/O 是否同线程，决定了阻塞能不能扩散到别的连接。")
    print("   olpt 不挂池 → 2.18s，I/O 完全停摆（阶段 3 之前的状态）")
    print("   olpt 挂池   → 0.19s，与 epoll 持平，同时保留 N 核 I/O（阶段 4 的形态）")
    print(" E1~E3 说明：分片只解决「多核并行」，不解决「队头阻塞」；")
    print(" 两个问题必须分开治 —— 分片管吞吐，业务池管尾延迟。")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
