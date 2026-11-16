/**
 * test/unit/frame_test.cpp —— 协议层纯内存单元测试（不碰 socket）
 *
 * 编译运行（不需要 cmake）：
 *   g++ -std=c++17 -Wall -Wextra -Werror -Ilib \
 *       test/unit/frame_test.cpp -o /tmp/frame_test && /tmp/frame_test
 *
 * 或走 suites：test/suites/06_frame.sh
 *
 * 【本文件的两条铁律】
 *  1. 只测 frame.hpp 真实存在的东西，绝不自己再造一个分帧器。
 *     早期版本里有个 MiniParser（6 字节头）——那是协议还没定稿时的草稿，
 *     后来 TLVDecoder 上线后它就变成了「测草稿」，测过也毫无意义。
 *  2. 长度断言一律写成 kHeaderSize + body.size()，
 *     不写魔法数字。包头从 6 → 24 时，写死 11 的断言全废。
 */

#include "frame.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

// ============================ 测试基础设施 ============================
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::printf("  [FAIL] %s   (line %d)\n", #cond, __LINE__);         \
        }                                                                      \
    } while (0)

// 与被测代码保持同一套常量，避免测试自己引入第二份真值
static constexpr size_t HDR = kHeaderSize;

// ============================ 被测消息类型 ============================
// 最小可用的 TLVMessage 子类：body 就是一段原样回传的字符串。
class EchoMessage : public TLVMessage {
public:
    static constexpr uint32_t kCmd = 0x00010001u;   // EchoReq

    std::string payload;

    EchoMessage() = default;
    explicit EchoMessage(std::string s) : payload(std::move(s)) {}

    uint32_t getCmd() const override { return kCmd; }
    std::string encodeValue() const override { return payload; }
    bool decodeValue(const uint8_t* d, size_t n) override {
        payload.assign(reinterpret_cast<const char*>(d), n);
        return true;
    }
};

// 便捷函数：解一个头出来做断言
static bool peek(const std::string& pkt, FrameHeader& h) {
    return decode_header(pkt.data(), pkt.size(), h);
}

// ============================ 1. pack 的字节精确性 ============================
static void test_pack_bytes() {
    std::printf("[1] pack 字节布局\n");

    EchoMessage m("hello");
    m.setSeq(42);
    m.setFlag(0x3);
    const std::string p = m.pack();

    CHECK(p.size() == HDR + 5);          // 24 + 5 = 29

    FrameHeader h;
    CHECK(peek(p, h));
    CHECK(h.magic == kMagic);
    CHECK(h.len == 5);
    CHECK(h.cmd == EchoMessage::kCmd);
    CHECK(h.seq == 42);
    CHECK(h.flag == 0x3);

    // 校验和必须覆盖 header[0..20) + body，用独立路径重算一遍比对
    CHECK(compute_checksum(h, p.data() + HDR, 5) == h.checksum);

    // 大端序落地检查：0x664A5455 在线上必须是 66 4A 54 55
    // 这条能抓出「忘了 htonl」这类只在跨机器才暴露的 bug
    CHECK(static_cast<uint8_t>(p[0]) == 0x66);
    CHECK(static_cast<uint8_t>(p[1]) == 0x4A);
    CHECK(static_cast<uint8_t>(p[2]) == 0x54);
    CHECK(static_cast<uint8_t>(p[3]) == 0x55);

    // body 从第 24 字节开始
    CHECK(p.compare(HDR, 5, "hello") == 0);
}

// ============================ 2. 空 body ============================
static void test_empty_body() {
    std::printf("[2] 空 body\n");

    const std::string p = EchoMessage("").pack();
    CHECK(p.size() == HDR);              // 24

    FrameHeader h;
    CHECK(peek(p, h));
    CHECK(h.len == 0);
    // 陷阱：adler32(空输入) == 1，但帧的 checksum 永远覆盖 20 字节头，
    // 所以任何合法帧的 checksum 都不可能等于 1。别拿 1 当哨兵。
    CHECK(h.checksum != 1);
    CHECK(h.checksum != 0);
}

// ============================ 3. 二进制 body（含 0x00） ============================
static void test_binary_body() {
    std::printf("[3] 二进制 body\n");

    // 里面故意放 0x00：任何用 strlen / %s / c_str() 处理 body 的实现都会在这挂掉
    const std::string bin{"\x00\xFF\x7F\x00", 4};
    const std::string p = EchoMessage(bin).pack();

    CHECK(p.size() == HDR + 4);          // 28

    FrameHeader h;
    CHECK(peek(p, h));
    CHECK(h.len == 4);
    CHECK(std::memcmp(p.data() + HDR, bin.data(), 4) == 0);
}

// ============================ 4. pack → decode → decodeValue 往返 ============================
static void test_roundtrip() {
    std::printf("[4] 编解码往返\n");

    EchoMessage out("顺便测一下中文和 emoji 😀");
    out.setSeq(0xDEADBEEF);
    out.setFlag(0xA5A5A5A5u);
    const std::string p = out.pack();

    TLVDecoder dec;
    dec.appendData(p.data(), p.size());

    FrameHeader h;
    std::string body;
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::OK);
    CHECK(body == out.payload);
    CHECK(h.seq == 0xDEADBEEF);
    CHECK(h.flag == 0xA5A5A5A5u);

    EchoMessage in;
    CHECK(in.decodeValue(reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    CHECK(in.payload == out.payload);

    // 消费干净
    CHECK(dec.pending() == 0);
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::NEED_MORE);
}

// ============================ 5. 粘包：两帧连发 ============================
static void test_sticky_frames() {
    std::printf("[5] 粘包两帧\n");

    const std::string f1 = EchoMessage("AAA").pack();
    const std::string f2 = EchoMessage("BBBB").pack();

    TLVDecoder dec;
    dec.appendData(f1.data(), f1.size());
    dec.appendData(f2.data(), f2.size());
    CHECK(dec.pending() == f1.size() + f2.size());

    FrameHeader h;
    std::string body;

    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::OK);
    CHECK(body == "AAA");
    CHECK(dec.pending() == f2.size());   // 只消费了第一帧

    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::OK);
    CHECK(body == "BBBB");
    CHECK(dec.pending() == 0);
}

// ============================ 6. 逐字节喂 ============================
static void test_byte_by_byte() {
    std::printf("[6] 逐字节喂\n");

    const std::string p = EchoMessage("drip").pack();

    TLVDecoder dec;
    FrameHeader h;
    std::string body;
    int ok_at = -1;

    for (size_t i = 0; i < p.size(); ++i) {
        dec.appendData(&p[i], 1);
        auto st = dec.tryDecode(h, body);
        if (st == TLVDecoder::Status::OK) {
            ok_at = static_cast<int>(i);
            break;
        }
        // 中途只能返回 NEED_MORE，绝不能返回 BAD_ERROR/ERROR
        CHECK(st == TLVDecoder::Status::NEED_MORE);
    }

    // 必须恰好收到最后一个字节才成帧
    CHECK(ok_at == static_cast<int>(p.size()) - 1);
    CHECK(body == "drip");
}

// ============================ 7. 包头被切断 ============================
static void test_header_split() {
    std::printf("[7] 包头跨包\n");

    const std::string p = EchoMessage("x").pack();

    TLVDecoder dec;
    FrameHeader h;
    std::string body;

    // 只给 10 字节（不足 24），必须 NEED_MORE 且一个字节都不能丢
    dec.appendData(p.data(), 10);
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::NEED_MORE);
    CHECK(dec.pending() == 10);

    dec.appendData(p.data() + 10, p.size() - 10);
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::OK);
    CHECK(body == "x");
    CHECK(dec.pending() == 0);
}

// ============================ 8. magic 错：整条流不可信 ============================
static void test_bad_magic() {
    std::printf("[8] magic 错误\n");

    // 典型场景：把 HTTP 请求误打到了 TLV 端口
    const char* http = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";   // 27 字节 > 24
    const size_t n = std::strlen(http);
    CHECK(n >= HDR);

    TLVDecoder dec;
    dec.appendData(http, n);

    FrameHeader h;
    std::string body;
    // magic 不对 → BAD_ERROR（流级不可信，调用方该关连接）
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::BAD_ERROR);
    // 缓冲必须被清空，否则下一次还会读到同样的脏头，死循环
    CHECK(dec.pending() == 0);
}

// ============================ 9. 校验和错：只丢这一帧 ============================
static void test_bad_checksum() {
    std::printf("[9] 校验和错误\n");

    std::string f1 = EchoMessage("bad").pack();
    f1[HDR] = static_cast<char>(f1[HDR] ^ 0xFF);   // 改 body，校验和必然对不上
    const std::string f2 = EchoMessage("good").pack();

    TLVDecoder dec;
    dec.appendData(f1.data(), f1.size());
    dec.appendData(f2.data(), f2.size());

    FrameHeader h;
    std::string body;

    // 分帧本身是好的，只是内容坏了 → ERROR（帧级错误，丢帧不丢连接）
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::ERROR);
    // 关键：坏帧被整帧吃掉，后面那帧必须还在
    CHECK(dec.pending() == f2.size());

    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::OK);
    CHECK(body == "good");
    CHECK(dec.pending() == 0);
}

// ============================ 10. len 超上限 ============================
static void test_len_over_limit() {
    std::printf("[10] len 超上限\n");

    FrameHeader fake;
    fake.magic = kMagic;
    fake.len   = kMaxBodyLen + 1;      // 谎报长度，实际 body 一个字节都没有
    fake.cmd   = EchoMessage::kCmd;
    const std::string hdr = encode_header(fake);

    TLVDecoder dec;
    dec.appendData(hdr.data(), hdr.size());

    FrameHeader h;
    std::string body;
    // 必须先验 len 再算 total，否则会傻等 10MB 数据（内存炸弹）
    CHECK(dec.tryDecode(h, body) == TLVDecoder::Status::BAD_ERROR);
    CHECK(dec.pending() == 0);
}

// ============================ 11. seq / flag ============================
static void test_seq_flag() {
    std::printf("[11] seq / flag\n");

    // 默认必须是 0，不能是栈上的垃圾值
    EchoMessage d;
    CHECK(d.getSeq() == 0);
    CHECK(d.getFlag() == 0);
    CHECK(d.pack().size() == HDR);     // 空 body

    EchoMessage m("z");
    m.setSeq(0);
    m.setFlag(0);
    FrameHeader h;
    CHECK(peek(m.pack(), h));
    CHECK(h.seq == 0);
    CHECK(h.flag == 0);

    // 边界值：0 和 0xFFFFFFFF 都要能过
    for (uint32_t v : {1u, 0x7FFFFFFFu, 0xFFFFFFFFu}) {
        EchoMessage t("v");
        t.setSeq(v);
        t.setFlag(v);
        FrameHeader th;
        CHECK(peek(t.pack(), th));
        CHECK(th.seq == v);
        CHECK(th.flag == v);
    }
}

// ============================ main ============================
int main() {
    std::printf("===== frame_test: 24 字节头 / adler32 / TLVDecoder =====\n");

    test_pack_bytes();
    test_empty_body();
    test_binary_body();
    test_roundtrip();
    test_sticky_frames();
    test_byte_by_byte();
    test_header_split();
    test_bad_magic();
    test_bad_checksum();
    test_len_over_limit();
    test_seq_flag();

    std::printf("----- pass=%d  fail=%d -----\n", g_pass, g_fail);
    if (g_fail == 0) std::printf("OK\n");
    return g_fail == 0 ? 0 : 1;
}