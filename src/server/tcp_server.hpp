#pragma once

#include <atomic>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <functional>
#include <unistd.h>

class TcpServerBase{
public:
    // 初始化列表顺序必须与成员声明顺序一致（_listen_fd 声明在 _port 之前），
    // 否则 -Wreorder 告警；这里两个都是平凡 int 所以无害，但成员间有依赖时就是 UB。
    TcpServerBase(int port): _listen_fd(-1),_port(port),_running(false){}
    virtual ~TcpServerBase(){stop();}

    void start(){
        create_socket();
        set_reuse_addr();
        do_bind();
        do_listen();
        _running = true;
        run_loop();
    }
    void stop(){
        /**
            两个线程交错执行时可能 close 两次同一个 fd，
            而第二次 close 时这个号可能已经被别的线程复用成客户端连接了 —— 那是灾难性的。
            解决方式：改成原子操作 这样exchange只有一方能拿到旧值
            我目前没改，等我看完多线程第五章后改
        */

        _running = false;
        if (_idle_fd >= 0) {::close(_idle_fd);_idle_fd = -1;}
        if(_listen_fd >= 0){close(_listen_fd);_listen_fd = -1;}
    }

    //设置业务
    using MessageCallback = std::function<void(int fd,const char* data,size_t len)>;
    void set_message_callback(MessageCallback cb){
        _msg_cb = std::move(cb);
    }

    /**
     * @brief 发送门面（供业务回调 / 外部调用）
     *
     * 为什么需要它：派生类的 send_wbuf 语义差异很大（线程模型是阻塞 send，
     * epoll 模型是「直发 + EAGAIN 进 wbuf 等 EPOLLOUT」），但调用方只持有
     * TcpServerBase*。而 send_wbuf 在基类里是 protected，静态类型检查会拦住
     * 外部调用，所以这里开一个 public 门面转调虚函数。
     */
    void send(int fd, const char* data, size_t len){
        if (fd >= 0 && data != nullptr && len > 0){
            send_wbuf(fd, data, len);
        }
    }
protected:

    /**
    纯虚函数
    1.在基类中抽象出一个方法，且该基类只能被继承，不能被实例化    
    2.这个方法必须被派生类实现
    满足这两个条件我们将其定义为纯虚函数
    写法：virtual void func() = 0;
    普通虚函数必须提供默认实现，然后子类再override重写
    */ 

    enum class AcceptStatus{
        Accept,
        RetryNow,
        Backoff,
        Stopped,
        Fatal
    };

    struct AcceptOutCome{
        AcceptStatus status;
        int fd;
        int err;
    };

    virtual void run_loop() = 0;//三种方法的核心差异

    
    //公共工具
    void create_socket();
    void set_reuse_addr();
    void do_bind();
    void do_listen();
    int accept_once(struct sockaddr_in& server_addr);
    //send with buf
    virtual void send_wbuf (int __fd,const void* __data,size_t __n){
        size_t sented = 0;
        //size_t __n = 1024;
        //char buf[__n];
        while (sented < __n){
            ssize_t n = ::send(__fd,static_cast<const char*>(__data) + sented,__n - sented,MSG_NOSIGNAL);
            if(n <= 0){
                if (n < 0 && errno == EINTR)
                {
                    continue;
                }else{
                    return;
                }
            }
            sented += static_cast<size_t>(n);
        }
    }

    //client 处理
    void handle_client_read(int client_fd);

    int _listen_fd;
    int _port;
    std::atomic<bool> _running; // 被信号处理函数、accept 循环、工作线程同时访问，必须原子
    MessageCallback _msg_cb;

    //预留fd
    int _idle_fd = -1;

};

