cd /tmp && cat > probe_http.cpp <<'EOF'
#include “frame.hpp”

// ------测试用的具体消息---------
class EchoMessage :public TLVMessage{
public:
    std::string payload;
    EchoMessage() = default;
    explicit EchoMessage(std:string s) : payload(std::move(s)){}

    uint16_t getType() const override{return 0x001;}
    std::string encodeValue() const override {return payload;}
    bool decodeValue(cosnt uint8_t* d,size_t n)override{
        payload.assign(reinterpret_cast<const char* >(d),n);
        return true;
    }
};

struct MiniParser {
    std::string buf;
    void feed(const char* d, size_t n) { buf.append(d, n); }
    bool next(uint16_t& type, std::string& value) {
        if (buf.size() < 6) return false;                  // header 不全
        uint16_t t; uint32_t l;
        std::memcpy(&t, buf.data(),     2);
        std::memcpy(&l, buf.data() + 2, 4);
        t = ntohs(t); l = ntohl(l);
        if (l > (1u << 20)) { printf("    !! len 超限 %u\n", l); buf.clear(); return false; }
        if (buf.size() < 6 + l) return false;              // body 不全
        type  = t;
        value = buf.substr(6, l);
        buf.erase(0, 6 + l);
        return true;
    }
};

