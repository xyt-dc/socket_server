#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
 tlv_probe.py —— TLV 协议层字节级验证库（纯函数，无网络）
============================================================================

【定位】
  这是 TLV 协议的**参考实现**，Python 侧把 24B 头 / adler32 / 拆包的规则
  独立写一遍。它的价值不是"再实现一次"，而是：

    C++ 的 frame.hpp 与 Python 的 pack() 必须产出**完全相同的字节**。
    任何一边改了偏移、字节序、校验范围，对照测试立刻红。

【协议速查】
  偏移  字段      宽度   说明
   0    magic     4B     0x664A5455
   4    len       4B     body 长度（不含 24B 头）
   8    cmd       4B     模块<<24 | 类型<<16 | 动作
  12    seq       4B     请求序号，响应必须原样回填
  16    flag      4B     位标志 bit0=压缩 bit1=加密
  20    checksum  4B     adler32( header[0..20) + body )

【adler32 的两个坑】
  1. 空输入 = 1，不是 0
  2. 初值是 1，不是 0  ← zlib.adler32 默认就是 1，但很多人手写 adler32 时写成 0

【作为库使用】
  from tlv_probe import pack, parse_frames, Cmd
============================================================================
"""

import struct
import zlib

# ---------------------------------------------------------------- 协议常量
MAGIC       = 0x664A5455
HEADER_SIZE = 24
MAX_BODY    = 10 * 1024 * 1024

# cmd 位布局：高 8 位=模块，中 8 位=类型，低 16 位=动作
class Module:
    SYSTEM = 0x00
    AUTH   = 0x01
    DEVICE = 0x02
    ORDER  = 0x03

class MsgType:
    REQUEST  = 0x01
    RESPONSE = 0x02
    NOTIFY   = 0x03

def cmd_of(module, msg_type, action):
    """拼出一个 cmd 值。例：cmd_of(0x00, 0x01, 1) == 0x00010001"""
    return (module << 24) | (msg_type << 16) | (action & 0xFFFF)

class Cmd:
    UNKNOWN  = 0x00000000
    ECHO_REQ = 0x00010001
    ECHO_RSP = 0x00020001
    PING_REQ = 0x00010002
    PING_RSP = 0x00020002   # C++ 侧目前错叫 PongResp，语义应为 Ping 的响应

# ---------------------------------------------------------------- 编码
def pack(cmd, body=b"", seq=0, flag=0, magic=MAGIC, corrupt_checksum=False):
    """把一个帧编成 24 + len(body) 字节。

    corrupt_checksum=True 时故意把校验和 +1，用来测服务端的丢帧逻辑。
    返回 bytes，长度恒等于 24 + len(body)。
    """
    if isinstance(body, str):
        body = body.encode()
    assert len(body) <= MAX_BODY, "body 超过 kMaxBodyLen"
    h20 = struct.pack(">IIIII", magic, len(body), cmd, seq, flag)
    cs = zlib.adler32(h20 + body) & 0xFFFFFFFF
    if corrupt_checksum:
        cs = (cs + 1) & 0xFFFFFFFF
    pkt = h20 + struct.pack(">I", cs) + body
    assert len(pkt) == HEADER_SIZE + len(body), "头部必须是 24 字节"
    return pkt

def unpack_header(pkt):
    """解 24B 头。返回 dict。"""
    if len(pkt) < HEADER_SIZE:
        raise ValueError("字节数不足 24，无法解头")
    m, ln, c, sq, fl, ck = struct.unpack(">IIIIII", pkt[:HEADER_SIZE])
    return dict(magic=m, len=ln, cmd=c, seq=sq, flag=fl, checksum=ck)

def verify_checksum(pkt):
    """重算校验和并与帧内比对。pkt 必须含完整 body。"""
    h = unpack_header(pkt)
    body = pkt[HEADER_SIZE:HEADER_SIZE + h["len"]]
    if len(body) != h["len"]:
        return False
    return (zlib.adler32(pkt[:20] + body) & 0xFFFFFFFF) == h["checksum"]

# ---------------------------------------------------------------- 解码（流式）
class Decoder:
    """增量分帧器 —— 对标 C++ 的 TLVDecoder。

    feed() 可以喂任意切分的字节流，next() 每次吐一个完整帧。
    这正是"粘包/拆包"测试要压的东西。
    """
    def __init__(self):
        self.buf = b""
        self.dropped = 0          # 被丢弃的坏帧计数

    def feed(self, data):
        self.buf += data
        return self

    def next(self):
        """返回 (header_dict, body) 或 None（数据不够）。
        坏帧抛 ValueError，但只丢这一帧，后面的仍可解。
        """
        if len(self.buf) < HEADER_SIZE:
            return None
        h = unpack_header(self.buf)
        if h["magic"] != MAGIC:
            self.buf = b""        # magic 错 -> 整个流不可信
            raise ValueError("bad magic: 0x%08X" % h["magic"])
        if h["len"] > MAX_BODY:
            self.buf = b""        # 长度非法 -> 同上
            raise ValueError("len 超限: %d" % h["len"])
        total = HEADER_SIZE + h["len"]
        if len(self.buf) < total:
            return None           # 半帧：一个字节都不许丢
        body = self.buf[HEADER_SIZE:total]
        if not verify_checksum(self.buf[:total]):
            self.buf = self.buf[total:]   # 只丢这一帧
            self.dropped += 1
            raise ValueError("checksum 不符（已丢该帧）")
        self.buf = self.buf[total:]       # 粘包：只消费一帧
        return h, body

    def drain(self):
        """把当前缓冲里所有完整帧取出来。返回 (frames, errors)。"""
        frames, errors = [], []
        while True:
            try:
                r = self.next()
            except ValueError as e:
                errors.append(str(e))
                continue
            if r is None:
                return frames, errors
            frames.append(r)


def main():
    """自测：纯内存，不碰网络。python3 tlv/tlv_probe.py"""
    ok = True
    def chk(name, cond):
        nonlocal ok
        print(("  [PASS] " if cond else "  [FAIL] ") + name)
        if not cond: ok = False

    print("== 1. 单帧往返 ==")
    p = pack(Cmd.ECHO_REQ, b"hello", seq=0x2A)
    chk("长度 = 24+5", len(p) == 29)
    h = unpack_header(p)
    chk("magic", h["magic"] == MAGIC)
    chk("len=5", h["len"] == 5)
    chk("cmd", h["cmd"] == Cmd.ECHO_REQ)
    chk("seq=0x2A", h["seq"] == 0x2A)
    chk("checksum 自洽", verify_checksum(p))

    print("== 2. 空 body ==")
    p0 = pack(Cmd.PING_REQ)
    chk("长度 = 24", len(p0) == 24)
    chk("校验和自洽", verify_checksum(p0))
    # ★ 注意：校验和永远覆盖 20 字节头，所以帧内的 checksum 不可能是 1。
    #   "空输入 adler32 = 1" 这个事实只体现在 compute_checksum 内部的初值上，
    #   不会出现在包头里。别拿 1 当哨兵判断"校验和未填"。
    chk("checksum != 1（因为有 20B 头参与计算）", unpack_header(p0)["checksum"] != 1)

    print("== 3. 粘包：两帧连发 ==")
    d = Decoder().feed(pack(Cmd.ECHO_REQ, b"AAA", 1) + pack(Cmd.ECHO_REQ, b"BBBB", 2))
    fs, errs = d.drain()
    chk("解出 2 帧", len(fs) == 2)
    chk("第一帧 body=AAA", fs[0][1] == b"AAA")
    chk("第二帧 body=BBBB", fs[1][1] == b"BBBB")
    chk("无残留", d.buf == b"")

    print("== 4. 拆包：逐字节喂 ==")
    raw = pack(Cmd.ECHO_REQ, b"split", 7)
    d = Decoder(); got = []
    for i in range(len(raw)):
        d.feed(raw[i:i+1])
        fs, _ = d.drain()
        got += fs
    chk("逐字节喂也解出 1 帧", len(got) == 1)
    chk("body=split", got and got[0][1] == b"split")

    print("== 5. 拆包：头切两半 ==")
    d = Decoder().feed(raw[:10])
    chk("10 字节解不出帧", d.next() is None)
    d.feed(raw[10:])
    fs, _ = d.drain()
    chk("补全后解出", len(fs) == 1)

    print("== 6. 坏校验和：只丢该帧 ==")
    good = pack(Cmd.ECHO_REQ, b"good", 1)
    bad  = pack(Cmd.ECHO_REQ, b"bad", 2, corrupt_checksum=True)
    d = Decoder().feed(good + bad + good)
    fs, errs = d.drain()
    chk("解出 2 个 good", len(fs) == 2)
    chk("记录 1 次丢帧", len(errs) == 1 and d.dropped == 1)

    print("== 7. 坏 magic：清空整个流 ==")
    d = Decoder().feed(b"GET / HTTP/1.1\r\n\r\n" + good)
    try:
        d.next(); chk("应抛异常", False)
    except ValueError:
        chk("抛 magic 异常", True)
    chk("缓冲被清空（HTTP 报文全丢）", d.buf == b"")

    print("== 8. len 超限 ==")
    huge = struct.pack(">IIIII", MAGIC, MAX_BODY + 1, Cmd.ECHO_REQ, 0, 0)
    huge += struct.pack(">I", 0)
    d = Decoder().feed(huge)
    try:
        d.next(); chk("应抛异常", False)
    except ValueError:
        chk("抛 len 超限", True)

    print("\n%s" % ("全部通过" if ok else "★ 有失败"))
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
