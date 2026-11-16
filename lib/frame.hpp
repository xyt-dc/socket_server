/**
 * 一般解决粘包问题有定长包，包头放长度检测和特殊字符三种选择
*  字节层面：24B 头怎么排、大端怎么转、adler32 怎么算、粘包拆包怎么切
*/

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <arpa/inet.h>

/**
 * 
 * TLV头协议设计：
 * 偏移  字段      宽度   说明
//  0     magic     4B     0x664A5455，快速识别 + 防误连
//  4     len       4B     body 长度（不含 24B 头）；必须 <= kMaxBodyLen
//  8     cmd       4B     业务指令，0x0501 = 发送文本
//  12    seq       4B     请求序号，0 表示无需响应
//  16    flag      4B     位标志：bit0 压缩 bit1 加密，bit2.. 保留
//  20    checksum  4B     adler32 / crc32c，覆盖 header[0..20) + body 
 * 
 */

// ---- 协议常量：全项目唯一定义处，不要散落到各处 ----
inline constexpr uint32_t kMagic      = 0x664A5455u;      // "BJTU"
inline constexpr size_t   kHeaderSize = 24;
inline constexpr uint32_t kMaxBodyLen = 10u * 1024 * 1024;

struct FrameHeader {
    uint32_t magic    = kMagic;
    uint32_t len      = 0;      // body 长度，不含 24B 头
    uint32_t cmd      = 0;
    uint32_t seq      = 0;
    uint32_t flag     = 0;
    uint32_t checksum = 0;
};

static_assert(sizeof(FrameHeader) == kHeaderSize,
              "FrameHeader 只用于内存表示；上线一律走 encode_header，禁止 memcpy 结构体");

// ---------------- Adler-32算法🌟🌟🌟 ----------------
// 空输入的校验值 = 1（不是 0），所以别拿 0 当"未填校验和"的哨兵。
inline uint32_t adler32_update(uint32_t adler, const uint8_t* data, size_t len) {
    //constexpr 这个值或函数在编译期就能算出来，不需要等到程序跑起来后再算
    constexpr uint32_t kMod = 65521;
    uint32_t a = adler & 0xFFFFu;
    uint32_t b = (adler >> 16) & 0xFFFFu;
    while (len > 0) {
        size_t n = len < 5552 ? len : 5552;      // 5552 = 保证 b 不溢出的最大块长
        len -= n;
        while (n-- > 0) { a += *data++; b += a; }
        a %= kMod;
        b %= kMod;
    }
    return (b << 16) | a;
}

inline uint32_t adler32(const uint8_t* data, size_t len) {
    return adler32_update(1u, data, len);
}

// ---------------- 序列化：唯一允许碰字节的地方 ----------------
inline std::string encode_header(const FrameHeader& h) {
    std::string out(kHeaderSize, '\0');
    auto put = [&out](size_t off, uint32_t v) {
        const uint32_t net = htonl(v);//设备小端序，网络大端序（高位字节在前）
        std::memcpy(&out[off], &net, sizeof(net));
    };
    put(0,  h.magic);
    put(4,  h.len);
    put(8,  h.cmd);
    put(12, h.seq);
    put(16, h.flag);
    put(20, h.checksum);
    return out;
}

// 返回 false 表示"字节还不够"，不是"帧非法"
inline bool decode_header(const char* buf, size_t avail, FrameHeader& h) {
    if (!buf || avail < kHeaderSize) return false;
    auto get = [buf](size_t off) {
        uint32_t v = 0;
        std::memcpy(&v, buf + off, sizeof(v));
        return ntohl(v);
    };
    h.magic    = get(0);
    h.len      = get(4);
    h.cmd      = get(8);
    h.seq      = get(12);
    h.flag     = get(16);
    h.checksum = get(20);
    return true;
}

// 校验和覆盖 header[0..20) + body：这样 len / cmd 被篡改也能发现
inline uint32_t compute_checksum(const FrameHeader& in, const char* body, size_t body_len) {
    FrameHeader h = in;
    h.checksum = 0;
    const std::string hdr = encode_header(h);
    uint32_t a = adler32_update(1u,
                                reinterpret_cast<const uint8_t*>(hdr.data()),
                                kHeaderSize - sizeof(uint32_t));
    if (body && body_len > 0)
        a = adler32_update(a, reinterpret_cast<const uint8_t*>(body), body_len);
    return a;
}
 

class TLVMessage {
public:
    virtual ~TLVMessage() = default;

    // 子类必须实现：业务指令号（4 Byte）
    virtual uint32_t getCmd() const = 0;
    // 子类必须实现：将自身的业务字段序列化为字节流（body）
    virtual std::string encodeValue() const = 0;
    // 子类必须实现：从字节流中反序列化还原业务字段
    virtual bool decodeValue(const uint8_t* data, size_t len) = 0;

    // 绝大多数消息用默认值，真有需要的子类再覆盖
    void setSeq(uint32_t s)noexcept{
        _seq = s;
    }
    void setFlag(uint32_t f)noexcept{
        _flag = f;
    }
    virtual uint32_t getSeq()  const { return _seq; }
    virtual uint32_t getFlag() const { return _flag; }

    // 基类统一提供：序列化。非虚 —— 全项目只该有一份实现
    std::string pack() const {
        const std::string body = encodeValue();
        if (body.size() > kMaxBodyLen) return {};        // body 超长，打包失败

        FrameHeader h;
        h.magic = kMagic;
        h.len   = static_cast<uint32_t>(body.size());
        h.cmd   = getCmd();
        h.seq   = getSeq();
        h.flag  = getFlag();
        h.checksum = compute_checksum(h, body.data(), body.size());   // 基类算，子类不用管

        std::string pkt = encode_header(h);
        pkt.append(body);
        return pkt;
    }
private:
    uint32_t _seq = 0;
    uint32_t _flag = 0;
};

class TLVDecoder {
public:
    enum class Status { OK, NEED_MORE, BAD_ERROR ,ERROR};

    TLVDecoder() = default;
    virtual ~TLVDecoder() = default;                 // 纯虚析构必须给定义，否则链接失败
    TLVDecoder(const TLVDecoder&) = delete;          // 分帧器持有连接状态，禁止拷贝
    TLVDecoder& operator=(const TLVDecoder&) = delete;
    TLVDecoder(TLVDecoder&&) = default;              // 但允许移动
    TLVDecoder& operator=(TLVDecoder&&) = default;

    void appendData(const char* data, size_t len) {
        if (data && len > 0) _buffer.append(data, len);
    }

    virtual Status tryDecode(FrameHeader& out_hdr, std::string& out_value) {
        if (_buffer.size() < kHeaderSize) return Status::NEED_MORE;

        FrameHeader h;
        if (!decode_header(_buffer.data(), _buffer.size(), h)) return Status::NEED_MORE;

        // ① magic 不对：这个流已经不可信，清空重来
        if (h.magic != kMagic) { _buffer.clear(); return Status::BAD_ERROR; }

        // ② 长度非法：同样清空。先验长度，绝不能先算 total 再等
        if (h.len > kMaxBodyLen) { _buffer.clear(); return Status::BAD_ERROR; }

        // 拆包：body 还没收齐，一个字节都不许丢
        const size_t total = kHeaderSize + h.len;
        if (_buffer.size() < total) return Status::NEED_MORE;

        // ③ 校验和覆盖 header[0..20) + body，len / cmd 被改也能发现
        if (compute_checksum(h, _buffer.data() + kHeaderSize, h.len) != h.checksum) {
            _buffer.erase(0, total);   // 分帧本身是好的，只丢这一帧，下一帧还能解析
            return Status::ERROR;
        }

        out_hdr   = h;
        out_value = _buffer.substr(kHeaderSize, h.len);

        // 粘包：只消费这一帧，剩下的留给下一次
        _buffer.erase(0, total);
        return Status::OK;
    }

    size_t pending() const { return _buffer.size(); }
    void   reset()         { _buffer.clear(); }

protected:
    // 派生类需要自己切帧时用（RawFramer 等）。返回引用，不拷贝
    std::string&       buffer()       { return _buffer; }
    const std::string& buffer() const { return _buffer; }

private:
    std::string _buffer;
};

