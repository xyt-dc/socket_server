#pragma once

#include <sys/epoll.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <iostream>
#include <cstring>
#include <string>
#include <vector>

class Epoll{
public:
    //1.RAII 封装 epoll_create1()
    //EPOLL_CLOEXEC 是传给 epoll_create1() 的一个标志位，作用是让 epoll 实例的文件描述符在 exec() 执行新程序时自动关闭‌，防止子进程继承用不到的 epoll 句柄
    explicit Epoll(int flags = EPOLL_CLOEXEC,int max_events = 1024):_max_events(max_events){
        _epoll_fd = ::epoll_create1(flags);
        if (_epoll_fd == -1){
            throw std::runtime_error("Failed to create epoll instance:"+std::string(strerror(errno)));
        }
    }

    ~Epoll(){
        if(_epoll_fd != -1){
            ::close(_epoll_fd);
        }
    }

    //禁止拷贝，防止文件描述符被多次关闭
    Epoll(const Epoll&) = delete;
    Epoll& operator=(const Epoll&) = delete;

    //允许移动语义
    //⚠️ 移动赋值必须 noexcept，否则 vector<Epoll> 扩容时优先走拷贝
    //   （拷贝已被 = delete）→ 直接编译失败
    Epoll(Epoll&& other) noexcept
        : _epoll_fd(other._epoll_fd), _max_events(other._max_events){
        other._epoll_fd = -1;
    }
    Epoll& operator=(Epoll&& other) noexcept{
        if(this != &other){
            if(_epoll_fd != -1){
                ::close(_epoll_fd);
            }
            _epoll_fd = other._epoll_fd;
            _max_events = other._max_events;
            other._epoll_fd = -1;
        }
        return *this;
    }

    //2.封装epoll_ctl，添加监听事件
    //add(增),mod(改),del(删),get_fd(查)

    bool add(int fd,uint32_t events = EPOLLIN){
        //添加监听事件
        return ctl(EPOLL_CTL_ADD,fd,events);
    }

    bool mod(int fd,uint32_t events = EPOLLIN||EPOLLOUT){
        //修改监听事件
        return ctl(EPOLL_CTL_MOD,fd,events);
    }

    bool del(int fd){
        //删除监听事件
        return ctl(EPOLL_CTL_DEL,fd,0);
    }

    int get_fd()const{
        //获取底层的epoll_fd
        return _epoll_fd;
    }

    int max_events()const{return _max_events;}

    void set_max_events(int n){
        if(n > 0)_max_events = n;
    }

    //3. 封装epoll wait (⚠️和recv的区别)
    //返回活跃事件的数量，事件列表通过引用返回
    using active_event = std::vector<struct epoll_event>;
    //⚠️ 非模板上下文里不能写 typename，C++17 下 clang 直接报错

    /**
     * 封装 epoll_wait
     * @param epoll_fd   epoll 实例的文件描述符
     * @param active_evs 用于接收触发事件的 vector 容器（引用传递）
     * @param max_events 本次最多监听多少个事件（通常等于 active_evs.size()）
     * @param timeout    超时时间（毫秒），-1 表示永久阻塞，0 表示立即返回
     * @return           返回触发的事件数量；若出错返回 -1；若超时返回 0
     */
    int wait(active_event& active_events,int timeout_ms = -1){
        if(active_events.empty()){
            //空缓冲区交出去必然 EFAULT，直接补齐到 _max_events
            active_events.resize(static_cast<size_t>(_max_events));
        }
        //maxevents 必须 <= 缓冲区容量，取两者较小值最安全
        const int maxev = static_cast<int>(
            std::min(active_events.size(),
                     static_cast<size_t>(_max_events)));

        for(;;){
            //vector::data() 返回数组首元素指针
            const int ready_count =
                ::epoll_wait(_epoll_fd,active_events.data(),maxev,timeout_ms);
            if(ready_count < 0){
                if(errno == EINTR){
                    continue;//信号打断重试
                }
                std::cerr<<"epoll_wait: "<<std::strerror(errno)<<std::endl;
                return -1;
            }
            //★ 清零尾部：上次跑的比这次多时，残留的旧事件会被误当成本轮有效事件
            for(int i = ready_count;i < maxev;++i){
                active_events[static_cast<size_t>(i)] = epoll_event{};
            }
            return ready_count;
        }
    }
private:
    int _epoll_fd = -1;
    int _max_events;

    /**
     * 封装 epoll_ctl 操作
     * @param op      操作类型：EPOLL_CTL_ADD (添加), EPOLL_CTL_MOD (修改), EPOLL_CTL_DEL (删除)
     * @param fd      要操作的目标文件描述符
     * @param events  关心的事件集合（如 EPOLLIN | EPOLLET），如果是删除操作可以传 0
     * @return        操作成功返回 true，失败返回 false
     */
    bool ctl(int op,int fd,uint32_t events){
        /**
         *  epoll_event ev{};
         * 相当于是对结构体全面清零
         * struct epoll_event {
                uint32_t     events;    // Epoll 事件类型（如 EPOLLIN, EPOLLOUT）
                epoll_data_t data;      // 用户数据，是一个联合体 Union
            };
         * 
        typedef union epoll_data {
            void        *ptr;
            int          fd;
            uint32_t     u32;
            uint64_t     u64;
        } epoll_data_t;

         */
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;

        if(epoll_ctl(_epoll_fd,op,fd,&ev) == -1){
            std::cerr<<"epoll_ctl error(op = "<<op<<",fd="<<fd<<"):"<<strerror(errno)<<std::endl;
            return false;
        }
        return true;
    }
};