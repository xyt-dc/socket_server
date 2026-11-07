#pragma once
#include "server/tcp_server.hpp"
#include "Iothread_pool.hpp"
#include <unordered_map>
#include <mutex>
#include "Connection.hpp"
#include "epoll.hpp"
#include <chrono>

/**
 * g++ -std=c++17 -fsyntax-only -I./lib -I./src src/mthread/IO_server.hpp
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
        if (_epfd >= 0){
            ::close(_epfd);
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
                if (fd == _listen_fd) {
                    handle_accept();
                } else if (event_poll[i].events & EPOLLIN) {      // ★ 先看可读
                    handle_read(fd);
                } else if (event_poll[i].events & (EPOLLERR | EPOLLHUP)) {
                    del_and_close(fd);
                }
            }
            //while (_pool.try_run_one())
        }
       

    }

private:
    bool epoll_init(){
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
                    // fd 耗尽：★ 绝对不能 continue（会 580,941 行日志那种忙等）
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
        while(true){
            int n = ::recv(fd,buf,sizeof(buf),0);
            if (n > 0) {
                if (_msg_cb) _msg_cb(fd, buf, static_cast<size_t>(n));
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
    int _epfd = -1;
    //IoThreadPool _pool;
    //map用于通过文件描述符（fd）快速查找对应 epoll_item（监听事件）的关联结构。
    std::unordered_map<int,std::shared_ptr<Connection>> _conns;
    std::mutex _mtx;
    std::vector<struct epoll_event> event_poll;
};


