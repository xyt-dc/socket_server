#pragma once
#include "server/tcp_server.hpp"
#include "Iothread_pool.hpp"
#include <unordered_map>
#include <mutex>
#include "Connection.hpp"
#include "epoll.hpp"
#include <chrono>
#include <cerrno>
#include <fcntl.h>

/**
 * g++ -std=c++17 -fsyntax-only -I./lib -I./src src/mthread/Epoll_server.hpp
 * 
 */

class EpollServer:public TcpServerBase{
    /** private: 只能由该类中的函数、其友元函数访问,不能被任何其他访问，该类的对象也不能访问.
        protected: 可以被该类中的函数、子类的函数、以及其友元函数访问,但不能被该类的对象访问
        public: 可以被该类中的函数、子类的函数、其友元函数访问,也可以由该类的对象访问
     */

public:
    static constexpr int kMaxEvents = 1024;
    static constexpr int kWaitTimeOut = 100;
    static constexpr size_t KReadBuf = 4096;
    
    EpollServer(int port):TcpServerBase(port){}
    ~EpollServer(){
        if (_epoll.get()->get_fd() >= 0){
            ::close(_epoll.get()->get_fd());
        }
    }

    void send_wbuf (int __fd,const void* __data,size_t __n)override{
        std::shared_ptr<Connection> conn_ptr;
        {
            std::lock_guard<std::mutex> lk(_mtx);

            auto it = _conns.find(__fd);
            if (it != _conns.end())
            {
                conn_ptr = it->second;
            }
        }

        // ★ 连接可能已经被 handle_read 里的 del_and_close 摘掉了
        //   （比如对端先 close，业务回调里还在发），此时直接丢弃。
        if (!conn_ptr) return;

        //1.如果wbuf里有上一次没发送完的数据，这次的数据排队到末尾
        if(!conn_ptr->wbuf_empty()){
            conn_ptr->append_wbuf(static_cast<const char*>(__data),__n);
            return;
        }

        //2.如果wbuf为空，尝试直接调用send发送
        size_t remaining = __n;
        ssize_t nwritten = 0;
        bool error = false;

        while(remaining > 0){
            nwritten = ::send(__fd, static_cast<const char*>(__data) + (__n - remaining), remaining, MSG_NOSIGNAL);
            if (nwritten > 0) {
                remaining -= static_cast<size_t>(nwritten);
            } else if (nwritten == 0) {
                // ★ send 返回 0 在 TCP 上意味着无法推进，继续循环就是死转
                //   （remaining 不变）。当成错误处理。
                error = true;
                break;
            } else {
                if (errno == EINTR) continue; // 信号打断，重试
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break; // 内核缓冲区满了，退出循环，转 wbuf
                }
                error = true; // 真正出错（如 EPIPE / ECONNRESET）
                break;
            }
        }

        if(error){
            del_and_close(__fd);
            return;
        }

        // 如果没发完就把剩下的残余数据塞进wbuf，并让epoll监听可写事件
        if(remaining > 0){
            conn_ptr->append_wbuf(static_cast<const char*>(__data) + (__n - remaining),remaining);
            _epoll->mod(__fd,EPOLLIN|EPOLLOUT);
        }
    }
    
protected:

    void run_loop() override{
        if(!epoll_init()){
            std::cerr<<"epoll_init failed"<<std::endl;
            return;
        }
        _epoll.get()->set_max_events(kMaxEvents);
        while(_running){
            //查看有多少活跃事件并返回
            int n = _epoll.get()->wait(event_poll,kWaitTimeOut);
            if (n<0)break;
            for(int i = 0; i < n;++i){
                int fd = event_poll[i].data.fd;
                const uint32_t ev = event_poll[i].events;

                if (fd == _listen_fd) {
                    handle_accept();
                    continue;
                }

                // ★ EPOLLERR/EPOLLHUP 优先：连接已死就没必要读写，
                //   且它常与 EPOLLIN 同时置位，放在 else-if 链末尾会被吞掉
                if (ev & EPOLLERR) {
                    del_and_close(fd);
                    continue;
                }

                // EPOLLHUP：对端 close。但内核里可能还残留未读完的数据，
                // 先读一次把数据榨干，读到 0 时 handle_read 内部会关闭。
                if (ev & EPOLLHUP) {
                    if (ev & EPOLLIN) handle_read(fd);
                    else              del_and_close(fd);
                    continue;
                }

                // ★ EPOLLIN / EPOLLOUT 用两个独立 if，不互斥：
                //   同一个 fd 完全可能同时可读可写
                if (ev & EPOLLIN)  handle_read(fd);
                if (ev & EPOLLOUT) handle_write(fd);
            }
            //while (_pool.try_run_one())
        }
    }

private:
    bool epoll_init(){
        event_poll.reserve(kMaxEvents);

        //设置listen_fd为非阻塞模式
        int flags = fcntl(_listen_fd,F_GETFL);
        fcntl(_listen_fd,F_SETFL,flags|O_NONBLOCK);

        //初始化一个epoll实例
        _epoll = std::make_unique<Epoll>(EPOLL_CLOEXEC,64);
        return  _epoll.get()->add(_listen_fd);
    }
    
    void handle_accept(){
        for(;;){
            //addr和addrlen都设置为nullptr是在只需要建立一个新链接拿到文件描述符不检验ip地址时的合法操作
            int cfd = ::accept4(_listen_fd,nullptr,nullptr,SOCK_NONBLOCK | SOCK_CLOEXEC);

            if(cfd < 0){
                if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }  // 队列空了，正常
                if (errno == EINTR) { continue; }                        // 信号打断
                if (errno == EMFILE || errno == ENFILE) {
                    std::cerr << "fd exhausted: " << std::strerror(errno) << '\n';
                    break;
                }
                std::cerr << "accept4: " << std::strerror(errno) << '\n';
                break;
            }
            {
                std::lock_guard<std::mutex> lk(_mtx);
                _conns.emplace(cfd,std::make_shared<Connection>(cfd));
            }
            if(!_epoll.get()->add(cfd)){
                del_and_close(cfd);
            }
        }
    }
    void handle_read(int fd){
        
        char buf[KReadBuf];
        //send_wbuf(fd,buf,KReadBuf);
        while(true){
            int n = ::recv(fd,buf,sizeof(buf),0);
            if (n > 0) {
                if (_msg_cb) _msg_cb(fd, buf, static_cast<size_t>(n));
                continue;
            }else if(n == 0){
                del_and_close(fd);
                return;
            }else {
                if (errno == EINTR) continue; // 被信号打断，重试
                if (errno == EAGAIN || errno == EWOULDBLOCK){
                    return;
                }
                del_and_close(fd);
                return;
            }
        }
    }
    void handle_write(int fd){
        //?
    } 

    void del_and_close(int fd){
        _epoll.get()->del(fd);
        std::lock_guard<std::mutex> lk (_mtx);
        _conns.erase(fd);
    }
    std::unique_ptr<Epoll> _epoll;
    //int _epfd = -1;
    //IoThreadPool _pool;
    //map用于通过文件描述符（fd）快速查找对应 epoll_item（监听事件）的关联结构。
    std::unordered_map<int,std::shared_ptr<Connection>> _conns;
    std::mutex _mtx;
    std::vector<struct epoll_event> event_poll;
};


