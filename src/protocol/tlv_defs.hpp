/**
    业务层面：有哪些消息。每条payload里装什么字段

*/


#pragma once

#include "frame.hpp"
#include <cstdint>
#include <string>
#include <utility>
#include <nlohmann/json.hpp>

// 高 8 位（Module ID：0x00 ~ 0xFF）—— 业务模块号
    // • 用于划分大的业务边界，甚至未来可以直接在网关层根据这个字节做微服务路由分发。
    // • 0x00：系统基础模块（心跳、握手、全局错误码）
    // • 0x01：用户/认证模块（登录、登出、Token 刷新）
    // • 0x02：设备控制/业务核心模块（传感器上报、指令下发）
    // • 0x03：订单/交易模块

// 中 8 位（Msg Type：0x00 ~ 0xFF）—— 交互模式类型
// 	• 用于区分这条消息的通信语义，方便中间件统一处理（如超时重试、日志级别控制）：
// 	• 0x01：Request（请求） —— 需要对方回复
// 	• 0x02：Response（响应） —— 对某个 Request 的回复
// 	• 0x03：Notify / Push（单向通知/推送） —— 发完即忘，不需要对方回复（如服务端主动推送状态、心跳上报）

// 低 16 位（Action ID：0x0001 ~ 0xFFFF）—— 具体动作编号
// 	• 在某个模块下的具体操作编号，最多支持 65,535 个接口，绰绰有余。

//举例 0x00 02 0001 : 00表示系统基础模块， 02表示对某个Request的回复， 0001 表示echo回声服务器

class Msg_frame {
public:
    enum class Cmd :uint32_t{
        //⚠️注意看上面的字节规划
        Unknown = 0x00000000, 
        EchoReq = 0x00010001,
        EchoResp = 0x00020001,
        PingReq = 0x00010002,
        PongResp = 0x00020002,
        
        //TODO Login,Logout,Chat,Trans
    };
};

//业务代码
class Msg_Handle_Echo : public TLVMessage{
public:
    Msg_Handle_Echo() = default;
    Msg_Handle_Echo(std::string msg,bool is_resp):_msg(msg),_is_resp(is_resp){}

    uint32_t getCmd() const override {
        return static_cast<uint32_t>(_is_resp? Msg_frame::Cmd::EchoResp:Msg_frame::Cmd::EchoReq);
    }

    std::string encodeValue() const override{
        return _msg;
    }

    bool decodeValue(const uint8_t* data, size_t len)override{
        if(!data && len)return false;
        //assign函数比=赋值更加安全
        _msg.assign(reinterpret_cast<const char*>(data),len);
        return true;
    }

private:
    std::string _msg;
    bool _is_resp = false;
    
};

// class Msg_Handle_Ping{
// public:

// private:

// };

//class Msg_Handle_LoginReq{};
//class Msg_Handle_LoginResp();