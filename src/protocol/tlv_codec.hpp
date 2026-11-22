/**
 * 
 * 这个类我建立的目的是想要把三个服务器n大于0的共同部分抽象出来，这样三个服务器之需要更改这个类的
 * 调用即可，增加代码复用率，以及增加安全性，但是修改过程中发现三种服务器中epoll和oneper处理逻辑一样，但是epoll并不一样
 * 并且最后还是一个连接持有一个TLVdeocer的话再增加一个类只是将decoder外面再封装一层，
 * 目的不明，暂时不做，但是后面肯定会做，因为现在的回调就是1对1的回调，要吧回调做成回调注册表
 * 不然只会空占着连接
 */

#pragma once

#include <iostream>
#include "frame.hpp"
#include <functional>
#include "server/tcp_server.hpp"


// class TLVCodecpp{
// public:
//     enum class Vedict{OK,StreamBroken};
//     using Send = std::function<void(int fd,const std::string str,int len)>;
//     using Handle = std::function<void(int fd,const FrameHeader& hdr,const std::string& body)>;

//     explicit TLVCodecpp(Send s):_sender(s){
        
//     }
    
//     TLVCodecpp::Vedict feed(int fd,const char* data,size_t n){
//         FrameHeader hdr;
//         std::string body;
//         bool broken = false;
//         _decoder.appendData(data,n);
//         for(;;){
//             auto st = _decoder.tryDecode(hdr,body);
//             if(st == TLVDecoder::Status::NEED_MORE)break;

//             if(st == TLVDecoder::Status::BAD_ERROR){
//                 broken = true;
//                 _dropped++;
//             }

//             if(st == TLVDecoder::Status::OK){
//                 //这里用uint32 放什么
//                 _handlers.emplace(,Handle(fd,hdr,body));
//             }
//         }
//     }

//     size_t dropped()const{return _dropped;}
//     void reset(){
//         _dropped = 0;
//     }
// private:
//     TLVDecoder _decoder;
//     std::unordered_map<uint32_t,Handle> _handlers;
//     Send _sender;
//     size_t _dropped = 0;
// };