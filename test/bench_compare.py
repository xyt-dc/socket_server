#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 bench_compare.py  ——  四模型对比表（base / thread / pool / epoll）
================================================================================

【它回答什么问题】
   「上了线程 / 线程池 / epoll，到底换来了什么？」
   单看 epoll 的 QPS 没有意义，必须有参照零点。base = 串行阻塞 = 那个零点。

【三个场景，各测一件事】

  1) hold      200 条空闲连接，只连不发，保持 4 秒
               采 → 线程数、RSS
               看点：epoll 是 1 个线程扛 200 连接，thread 是 201 个线程
               ⚠️ base 在这个场景会「连不上全部」—— 因为它卡在第 1 条连接上，
                  后面的连接只能躺在 accept 队列里，这不是缺陷，是下限本身

  2) latency   20 连接 × 300 消息 × 256 B，ping-pong（一问一答）
               采 → p50 / p99 / msg_per_s
               看点：这是最公平的延迟对比。
                    thread 每连接一个线程，通常 RTT 最好；
                    epoll 单线程要在 20 条连接间切换，反而可能更慢。
                    **表里必须诚实呈现这一点**，不能只挑 epoll 赢的场景。

  3) pipeline  20 连接 × 2000 消息 × 4096 B（160 MB），一次写完再读
               采 → msg_per_s / p99，或标 DNF（Did Not Finish）
               看点：**这是 epoll 的主场**。
                    thread 模型在阻塞 send 下会与客户端形成双向缓冲互锁：
                      服务端 Send-Q 涨到 3.6 MB → 阻塞在 send()
                      客户端 Recv-Q 涨到 100 KB → 客户端在 drain() 里等
                      两边都不读 → 死锁
                    pool(4) 因为只有 4 个 worker，同时阻塞的连接数受限，能活
                    epoll 有 wbuf + EPOLLOUT，服务端永不阻塞，最优

【DNF 怎么判定】
   timeout 到期 = DNF。不是「失败」，是「这个模型在这个场景下不具备该能力」。
   表里如实写 DNF，不写 0，也不写「无数据」—— 0 会被误读成「很快」。

【用法】
    python3 test/bench_compare.py                    # 全部四个模型 × 三场景
    python3 test/bench_compare.py --models=base,epoll # 只跑指定模型
    python3 test/bench_compare.py --scenarios=hold,latency
    python3 test/bench_compare.py --json             # 输出 JSON 便于二次处理
    python3 test/bench_compare.py --no-build         # 跳过编译
================================================================================
"""
import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(ROOT, "build", "bench_server")
CLIENT = os.path.join(ROOT, "test", "bench_client.py")

ALL_MODELS = ["base", "thread", "pool", "epoll"]
ALL_SCENARIOS = ["hold", "latency", "pipeline"]

# ---- 场景参数（集中在这里，改规模只改这一处）----
SCEN = {
    # name:      mode,        conns, msgs, size, hold, timeout
    "hold":      dict(mode="hold",       conns=200, msgs=1,    size=64,   hold=4.0, timeout=60),
    "latency":   dict(mode="latency",    conns=20,  msgs=300,  size=256,  hold=0,   timeout=90),
    "pipeline":  dict(mode="throughput", conns=20,  msgs=2000, size=4096, hold=0,   timeout=60),
}

# base 模型在 hold 场景下只能服务 1 条连接，其余排队。
# 它的 hold 值必须比别的模型长一点，否则还在排队就被我们 kill 了，
# 采到的线程数/RSS 没有代表性。这里单独放大。
BASE_HOLD_EXTRA = 0.0


def log(msg=""):
    print(msg, flush=True)


def free_port(start=6500, span=400):
    """找一个 bind 得上的端口。比随机数靠谱：随机到被占的概率不能忽略。"""
    for p in range(start, start + span):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free port")


def kill_port_owner(port):
    """兜底：SIGKILL 任何还占着这个端口的 bench_server。

    为什么要 SIGKILL：bench_server 装了 SIGTERM handler 做优雅停机，
    SIGTERM 可能被吞掉（尤其卡在阻塞 send 时），进程变成孤儿继续占端口。
    """
    try:
        out = subprocess.run(
            ["pgrep", "-f", f"bench_server.*--port={port}"],
            capture_output=True, text=True, timeout=5,
        ).stdout.split()
        for pid in out:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except (ProcessLookupError, ValueError):
                pass
    except Exception:
        pass


class Server:
    def __init__(self, model, port, workers=4):
        self.model, self.port, self.workers = model, port, workers
        self.proc = None
        self.log_path = f"/tmp/bc_{model}_{port}.log"

    def start(self):
        cmd = [BENCH, f"--server={self.model}", f"--port={self.port}"]
        if self.model == "pool":
            cmd += [f"--workers={self.workers}", "--queue-cap=1024"]
        self.fh = open(self.log_path, "wb")
        self.proc = subprocess.Popen(cmd, stdout=self.fh, stderr=subprocess.STDOUT)

        # 等端口真的 listen 上（不是 sleep 固定时间）
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.model} 启动即退出，见 {self.log_path}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    return True
            time.sleep(0.05)
        raise RuntimeError(f"{self.model} 端口 {self.port} 10 秒内没起来")

    def sample(self):
        """采线程数 + RSS。进程已经死了就返回 0/0。"""
        if self.proc is None or self.proc.poll() is not None:
            return 0, 0
        pid = self.proc.pid
        try:
            nthreads = len(os.listdir(f"/proc/{pid}/task"))
        except OSError:
            nthreads = 0
        rss = 0
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss = int(line.split()[1])
                        break
        except OSError:
            pass
        return nthreads, rss

    def stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.kill()          # 直接 SIGKILL，不等优雅退出
                self.proc.wait(timeout=5)
            except Exception:
                pass
        kill_port_owner(self.port)        # 兜底：清掉任何残留
        try:
            self.fh.close()
        except Exception:
            pass


def run_client(port, sc, timeout, srv, sample_interval=0.1):
    """跑一次客户端，**同时**在本进程里持续采样服务端资源峰值。

    为什么必须边跑边采（这是第一版脚本最贵的错误）：
      hold 场景下客户端做完 ``hold`` 秒就关闭全部连接，服务端随之回收所有
      线程/fd。等客户端退出了再去 ``/proc/<pid>/task`` 数线程，只能数到 1。
      实测：thread 模型 50 条连接，连接保持期间是 51 线程，客户端退出后剩 1 线程。
      所以采样必须在窗口内做，并且**取峰值**（线程是逐步 spawn 的，单次采样会偏低）。

    返回 (status, data, elapsed, peak_threads, peak_rss_kb)。
    """
    cmd = [
        sys.executable, CLIENT, f"--port={port}", f"--mode={sc['mode']}",
        f"--conns={sc['conns']}", f"--msgs={sc['msgs']}", f"--size={sc['size']}",
    ]
    if sc["mode"] == "hold":
        cmd.append(f"--hold={sc['hold']}")
    cmd.append("--json")

    peak_threads = 0
    peak_rss = 0
    t0 = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        while p.poll() is None:
            th, rss = srv.sample()
            peak_threads = max(peak_threads, th)
            peak_rss = max(peak_rss, rss)
            if time.time() - t0 > timeout:
                p.kill()
                p.wait(timeout=5)
                return "DNF", None, timeout, peak_threads, peak_rss
            time.sleep(sample_interval)
        elapsed = time.time() - t0
        raw = (p.stdout.read() or "").strip()
    finally:
        try:
            p.stdout.close()
        except Exception:
            pass

    # 收尾再采一次：客户端刚退出、服务端还没清完的瞬态也可能更高
    th, rss = srv.sample()
    peak_threads = max(peak_threads, th)
    peak_rss = max(peak_rss, rss)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return "ERR", None, elapsed, peak_threads, peak_rss
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            return "ERR", None, elapsed, peak_threads, peak_rss

    bad = data["mismatch"] > 0 or data["errors"] > 0 or data["partial"] > 0
    return ("BAD" if bad else "OK"), data, elapsed, peak_threads, peak_rss


def measure(model, scenario):
    """跑一个 (模型, 场景) 组合，返回一行结果。"""
    sc = SCEN[scenario]
    port = free_port()
    srv = Server(model, port)
    row = dict(model=model, scenario=scenario, status="?", threads=0, rss_kb=0,
               msg_per_s=0.0, p50_ms=0.0, p99_ms=0.0, recv=0, sent=0,
               conn_ok=0, mismatch=0, errors=0, partial=0, eof_early=0,
               elapsed_s=0.0)

    try:
        srv.start()
        # 线程数/RSS 在客户端运行**期间**采样并取峰值（原因见 run_client 的注释）
        status, data, elapsed, peak_threads, peak_rss = run_client(
            port, sc, sc["timeout"], srv)
        row["elapsed_s"] = round(elapsed, 3)
        row["threads"] = peak_threads
        row["rss_kb"] = peak_rss

        if status == "DNF":
            row["status"] = "DNF"
        elif status == "ERR":
            row["status"] = "ERR"
        else:
            row["status"] = status
            row.update(
                msg_per_s=data["msg_per_s"], p50_ms=data["p50_ms"],
                p99_ms=data["p99_ms"], recv=data["recv"], sent=data["sent"],
                conn_ok=data["conn_ok"], mismatch=data["mismatch"],
                errors=data["errors"], partial=data["partial"],
                eof_early=data["eof_early"],
            )
    except Exception as e:
        row["status"] = "ERR"
        row["err"] = str(e)
    finally:
        srv.stop()
        time.sleep(0.4)      # 让端口彻底释放，避免下一个模型 TIME_WAIT

    return row


def fmt_cell(v, width, align=">"):
    s = str(v)
    return f"{s:{align}{width}}"


def print_table(rows):
    models_order = [m for m in ALL_MODELS if any(r["model"] == m for r in rows)]

    for scenario in ALL_SCENARIOS:
        sub = [r for r in rows if r["scenario"] == scenario]
        if not sub:
            continue
        sc = SCEN[scenario]
        log()
        log(f"── 场景 {scenario}  ({sc['mode']}, {sc['conns']} 连接 × {sc['msgs']} 消息 × {sc['size']} B) ──")

        if scenario == "hold":
            log(f"  {'模型':<8}{'状态':<8}{'线程数':>8}{'RSS(KB)':>10}{'建连成功':>10}")
            log("  " + "-" * 46)
            for m in models_order:
                r = next((x for x in sub if x["model"] == m), None)
                if not r:
                    continue
                log(f"  {m:<8}{r['status']:<8}{r['threads']:>8}{r['rss_kb']:>10}{r['conn_ok']:>10}")
        else:
            log(f"  {'模型':<8}{'状态':<8}{'msg/s':>10}{'p50(ms)':>10}{'p99(ms)':>10}{'recv':>8}")
            log("  " + "-" * 56)
            for m in models_order:
                r = next((x for x in sub if x["model"] == m), None)
                if not r:
                    continue
                if r["status"] == "DNF":
                    log(f"  {m:<8}{'DNF':<8}{'—':>10}{'—':>10}{'—':>10}{'—':>8}")
                else:
                    log(f"  {m:<8}{r['status']:<8}{r['msg_per_s']:>10}"
                        f"{r['p50_ms']:>10}{r['p99_ms']:>10}{r['recv']:>8}")

    log()
    log("  状态说明：")
    log("    OK  = 全部数据正确回显（mismatch=errors=partial=0）")
    log("    BAD = 有数据不完整/内容错误 → 该模型在这场景下不可用")
    log("    DNF = Did Not Finish，超时未完成 → 不具备该场景的能力，不是「0 吞吐」")
    log("    ERR = 启动失败 / 客户端崩溃，看日志")


def main():
    ap = argparse.ArgumentParser(description="四模型对比基准")
    ap.add_argument("--models", default=",".join(ALL_MODELS))
    ap.add_argument("--scenarios", default=",".join(ALL_SCENARIOS))
    ap.add_argument("--workers", type=int, default=4, help="pool 的 worker 数")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-build", action="store_true")
    args = ap.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    for m in models:
        if m not in ALL_MODELS:
            log(f"unknown model: {m}（可选 {ALL_MODELS}）")
            return 2
    for s in scenarios:
        if s not in ALL_SCENARIOS:
            log(f"unknown scenario: {s}（可选 {ALL_SCENARIOS}）")
            return 2

    if not args.no_build:
        log("[build] cmake --build build --target bench_server")
        p = subprocess.run(["cmake", "--build", os.path.join(ROOT, "build"),
                            "--target", "bench_server", "-j"],
                           cwd=ROOT, capture_output=True, text=True)
        if p.returncode != 0:
            log(p.stdout[-3000:])
            log(p.stderr[-3000:])
            return 2

    if not os.path.exists(BENCH):
        log(f"missing {BENCH}，先构建")
        return 2

    global BASE_HOLD_EXTRA
    total = len(models) * len(scenarios)
    rows = []
    n = 0
    for scenario in scenarios:
        for model in models:
            n += 1
            log(f"[{n}/{total}] {model} × {scenario} ...")
            rows.append(measure(model, scenario))

    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    else:
        print_table(rows)

    # 退出码：只要有一个模型在某场景下 BAD 就返回 1（DNF 不算失败，是已知能力边界）
    bad = any(r["status"] == "BAD" for r in rows)
    err = any(r["status"] == "ERR" for r in rows)
    return 1 if (bad or err) else 0


if __name__ == "__main__":
    sys.exit(main())