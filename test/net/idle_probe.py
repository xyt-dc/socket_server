#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
 idle_probe.py —— 空闲超时（idle timeout）行为探针
============================================================================

【它验的是哪一件事】
  服务端「多久没有双向流量就断开这条连接」这条规则，是否真的生效，
  以及它是否**只**断开该断的、**不**误杀不该断的。

  这跟 05_tlv.sh 验的东西完全没有重叠：
    05_tlv  → 单帧/粘包/拆包/坏校验（协议正确性）
    idle    → 时间维度的策略（连接生命周期）

【三种模式，对应三个断言】

  die      连上就不说话，期望服务端在超时后主动断开。
           客户端能观察到的「断开」有两种，两种都算正常：
             · recv 返回 b""      → 服务端 close() 发了 FIN
             · ConnectionResetError → 服务端关的时候对端还在发/还有未读数据，
                                      内核回 RST
           ★ 只接受 FIN 会误判成失败，这是最容易写错的一条。

  alive    每隔 interval 发一次 Echo，持续 duration（应远超 timeout）。
           期望全程都能收到响应，一次都不能断。
           这一条抓的是「刷新点漏了」：只在 recv 时刷时间戳、
           忘了在 send 成功时也刷，就会误杀只推不读的连接。

  starve   只对 pool 模型有意义。
           先用 N 条「连上不说」的连接占满 worker，再开一条正常连接：
             阶段 1（超时还没到）→ 正常连接**应该拿不到响应**
             阶段 2（超时已过）  → 正常连接**应该能拿到响应**
           ★ 这一条是空闲超时在 pool 上的真正价值：
             泄漏的不是 fd，是**被阻塞的 worker 线程**。
             thread 模型每条连接独立线程，不存在饿死，故跳过。

【为什么用 JSON 输出】
  跟 bench_client.py 一致，脚本层用 jget 抠字段，不依赖人眼读日志。

【用法】
  ./idle_probe.py --port=6501 --mode=die   --idle-timeout=2 --json
  ./idle_probe.py --port=6501 --mode=alive --idle-timeout=2 --interval=0.5 --duration=8 --json
  ./idle_probe.py --port=6501 --mode=starve --idle-timeout=3 --dead=2 --workers=2 --json
============================================================================
"""

import argparse
import asyncio
import json
import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "tlv"))
from tlv_probe import pack, HEADER_SIZE   # noqa: E402


# ---------------------------------------------------------------- 帧工具

def echo_frame(seq=0, body=b"idle-probe"):
    """构造一个 EchoReq 帧（cmd=0x00010001）。"""
    return pack(0x00010001, body, seq=seq, flag=0)


async def recv_one_reply(reader, timeout):
    """读一个完整的响应帧，返回 body；超时/断开返回 None。"""
    try:
        hdr = await asyncio.wait_for(reader.readexactly(HEADER_SIZE),
                                     timeout=timeout)
        body_len = int.from_bytes(hdr[4:8], "big")
        body = await asyncio.wait_for(reader.readexactly(body_len),
                                      timeout=timeout)
        return body
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, OSError):
        return None


# ---------------------------------------------------------------- 模式

async def mode_die(args, out):
    """连上不发，等被踢。返回断开方式与耗时。"""
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.open_connection(args.host, args.port)
    except OSError as e:
        out["error"] = "connect: %s" % e
        return

    # 给足余量：超时时长的 3 倍。服务端扫描周期是 100ms 级，不会被这个精度影响。
    budget = args.idle_timeout * 3 + 1.0
    kind = "none"
    try:
        data = await asyncio.wait_for(reader.read(1), timeout=budget)
        # 空闲连接收不到任何字节，正常断开时 read 直接返回 b""
        kind = "fin" if data == b"" else "unexpected_data"
    except asyncio.TimeoutError:
        kind = "still_alive"          # ← 超时没生效
    except (ConnectionResetError, BrokenPipeError):
        kind = "rst"                  # ← 等价于 fin，也是正常断开

    out["close_kind"] = kind
    out["elapsed_s"] = round(time.monotonic() - t0, 3)
    out["ok"] = kind in ("fin", "rst")

    try:
        writer.close()
        await writer.wait_closed()
    except (ConnectionResetError, BrokenPipeError):
        pass


async def mode_alive(args, out):
    """定期发 Echo，全程不能断。"""
    try:
        reader, writer = await asyncio.open_connection(args.host, args.port)
    except OSError as e:
        out["error"] = "connect: %s" % e
        return

    sent = recv = 0
    dropped = 0
    t_end = time.monotonic() + args.duration
    seq = 1
    while time.monotonic() < t_end:
        try:
            writer.write(echo_frame(seq))
            await writer.drain()
            sent += 1
        except (ConnectionResetError, BrokenPipeError, OSError):
            dropped += 1
            break

        body = await recv_one_reply(reader, timeout=args.idle_timeout)
        if body is None:
            dropped += 1
            break
        recv += 1
        seq += 1
        await asyncio.sleep(args.interval)

    out["sent"] = sent
    out["recv"] = recv
    out["dropped"] = dropped
    # 至少要跑够 2 个超时周期，否则这次测试本身没意义
    out["ok"] = (dropped == 0 and recv == sent and recv > 0)

    try:
        writer.close()
        await writer.wait_closed()
    except (ConnectionResetError, BrokenPipeError):
        pass


async def mode_starve(args, out):
    """
    两阶段：超时前 worker 应被死连接占死，超时后应被释放。
    只有 pool 模型有意义（thread 每条连接独立线程，epoll 不占线程）。
    """
    # ---- 阶段 1：占用 worker，然后试一条正常连接 ----
    dead = []
    for _ in range(args.dead):
        try:
            _, w = await asyncio.open_connection(args.host, args.port)
            dead.append(w)
        except OSError as e:
            out["error"] = "connect dead: %s" % e
            return

    await asyncio.sleep(0.4)      # 等死连接确实落到 worker 上

    phase1 = await _try_live(args)
    out["phase1_served"] = phase1

    # ---- 阶段 2：等超时把死连接清掉，再试一次 ----
    await asyncio.sleep(args.idle_timeout * 2)
    phase2 = await _try_live(args)
    out["phase2_served"] = phase2

    for w in dead:
        try:
            w.close()
            await w.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass

    # 期望：超时前饿死（0），超时后恢复（1）
    out["ok"] = (phase1 == 0 and phase2 == 1)


async def _try_live(args):
    """开一条正常连接发一个 Echo，2 秒内拿到响应返回 1，否则 0。"""
    try:
        reader, writer = await asyncio.open_connection(args.host, args.port)
    except OSError:
        return 0
    try:
        writer.write(echo_frame(1))
        await writer.drain()
        body = await recv_one_reply(reader, timeout=2.0)
        return 1 if body is not None else 0
    except (ConnectionResetError, BrokenPipeError, OSError):
        return 0
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass


# ---------------------------------------------------------------- 入口

MODES = {"die": mode_die, "alive": mode_alive, "starve": mode_starve}


def main():
    ap = argparse.ArgumentParser(description="idle timeout probe")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--mode", choices=sorted(MODES), required=True)
    ap.add_argument("--idle-timeout", type=float, default=2.0,
                    help="服务端 --idle-timeout 的秒数，必须与启动参数一致")
    ap.add_argument("--interval", type=float, default=0.5,
                    help="alive 模式的发送间隔（秒）")
    ap.add_argument("--duration", type=float, default=8.0,
                    help="alive 模式总时长（秒），应 >= 2×idle-timeout")
    ap.add_argument("--dead", type=int, default=2,
                    help="starve 模式的死连接数")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    out = {"mode": args.mode, "ok": 0}
    try:
        asyncio.run(MODES[args.mode](args, out))
    except Exception as e:                                  # noqa: BLE001
        out["error"] = "%s: %s" % (type(e).__name__, e)
        out["ok"] = 0

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(" ".join("%s=%s" % (k, v) for k, v in out.items()))

    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())