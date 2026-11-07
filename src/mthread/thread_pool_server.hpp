/**
 * 和one per connect 相比只有accept后的逻辑不同，其余统统可以复用
 * 但是引入线程池后也有几点新问题需要考虑，
 * 1.生命周期
 * 2.优雅关闭
 * 3.client的并发窗口
 * 
 */
#pragma once

#include "server/tcp_server.hpp"
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include "jthread.hpp"
#include "Iothread_pool.hpp"
#include <unordered_set>

const size_t thread_count = std::thread::hardware_concurrency();

class ThreadPool_server : public TcpServerBase{
public:
    ThreadPool_server(int port,size_t workers = thread_count,size_t queue_cap = 1024):TcpServerBase(port),_Pool(workers,queue_cap){}

    ~ThreadPool_server(){
        _Pool.shutdown();
    }

    void cleanup_clients(){
        {
            std::lock_guard<std::mutex> lk (_mtx);
            for(int fd : _clients){
                ::shutdown(fd,SHUT_RDWR);
            }
        }
        _Pool.shutdown();
    }
protected:
    // errno -> 状态的无状态翻译。放这里而不是基类，理由见文件头的「分层理由」。
    static AcceptStatus classify_accept(int fd, int err){
        if (fd >= 0)                  return AcceptStatus::Accept;
        if (!err)                     return AcceptStatus::Fatal;
        switch (err){
            case EINTR:                   // 被信号打断（含 stop() 的 shutdown 唤醒）
            case ECONNABORTED:            // 三次握手期间被对端 abort
            case EPROTO:                  return AcceptStatus::RetryNow;
            case EMFILE:                  // 本进程 fd 用尽
            case ENFILE:                  // 全系统 fd 用尽
            case ENOBUFS:
            case ENOMEM:                  return AcceptStatus::Backoff;
            case EBADF:                   // listen_fd 已被 close
            case EINVAL:                  // socket 已 shutdown（实测 errno=22）
            case ENOTSOCK:                return AcceptStatus::Stopped;
            default:                      return AcceptStatus::Fatal;
        }
    }

    void run_loop() override{

        const auto kBackoffMin = std::chrono::milliseconds(100);
        const auto kBackoffMax = std::chrono::milliseconds(800);
        auto last_log = std::chrono::steady_clock::now() - std::chrono::seconds(5);
        std::chrono::milliseconds backoff = kBackoffMin;
        unsigned int err_count = 0;

        while(_running){
            sockaddr_in client_addr;
            // accept_once 只返回 int，失败时 errno 由本线程读取（errno 是线程局部的）
            int fd = accept_once(client_addr);
            const int saved_errno = (fd < 0) ? errno : 0;
            AcceptOutCome res_fd{classify_accept(fd, saved_errno), fd, saved_errno};

            if (res_fd.status == TcpServerBase::AcceptStatus::Accept){
                backoff = kBackoffMin;
                if (err_count>0){
                    std::cerr << "[accept] recovered,this error count = "<<err_count<<std::endl;
                }
                err_count = 0;
                {
                    std::lock_guard<std::mutex> lk(_mtx);
                    _clients.emplace(res_fd.fd);
                }
                
                bool acc = _Pool.submit([this,fd = res_fd.fd](){
                        handle_client_read(fd);
                    });
                if(!acc){
                    std::lock_guard<std::mutex> lk (_mtx);
                    std::cerr<<"queue is empty "<<std::endl;
                    _clients.erase(res_fd.fd);
                    ++_reject_count;
                    close(res_fd.fd);
                }

                //detach（）退出要等到主函数声明周期结束但是this指针指向的当前的类，res_fd也是local_var不会报错吗？
            }else if (res_fd.status == TcpServerBase::AcceptStatus::Backoff) {
                ++err_count;
                const auto now = std::chrono::steady_clock::now();
                if (now - last_log >= std::chrono::seconds(1)){
                    std::cerr<<"[accept]"<<std::strerror(res_fd.err)<<"accumulate:"<<err_count<< "backoff:"<<backoff.count()<<std::endl;
                    last_log = now;
                }
                // 注意：这里原本调用 reject_one_client()（预留 fd + 踢一个连接）。
                // 那套「预留 fd」技巧需要基类持有 _idle_fd，当前基类不放它 ——
                // 因为它是「策略」不是「原语」，放到子类才符合只改 run_loop 的设计。
                // 现在只有退避，EMFILE 期间新连接会被内核拒绝，客户端自己重连。
                std::this_thread::sleep_for(backoff);
                backoff = std::min(backoff*2,kBackoffMax);
            }else if (res_fd.status == TcpServerBase::AcceptStatus::Stopped){
                break;
            }else if (res_fd.status == TcpServerBase::AcceptStatus::RetryNow){
                continue;
            }else if (res_fd.status == TcpServerBase::AcceptStatus::Fatal){
                ++err_count;
                const auto now = std::chrono::steady_clock::now();
                if (now - last_log >= std::chrono::seconds(1)){
                    std::cerr<<"[accept]"<<std::strerror(res_fd.err)<<"accumulate:"<<err_count<< "backoff:"<<backoff.count()<<std::endl;
                    last_log = now;
                }
                std::this_thread::sleep_for(backoff);
                backoff = std::min(backoff*2,kBackoffMax);
            }
        }
        cleanup_clients();
    }

private:
//注意⚠️子类不要和父类去同样的名字，这是隐藏不是重载，重载必须不同参数
    void handle_client_read(int client_fd){
        char buf[4096];
        while (true) {
            // 不能用 MSG_PEEK：只窥探不消费，同一份数据会被无限次读到 -> 死循环
            ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
            if (n > 0) {
                if (_msg_cb) _msg_cb(client_fd, buf, static_cast<size_t>(n));
            } else if (n == 0) {
                std::cout << "client " << client_fd << " closed" << std::endl;
                break;
            } else {
                if (errno == EINTR) continue; // 被信号打断，重试
                perror("recv");
                break;
            }
        }
        //close(client_fd);
        //收尾，FIN（final传完了） 错误都从这走
        {
            std::lock_guard<std::mutex> lk(_mtx);
            _clients.erase(client_fd);
            ::close(client_fd);
        }
    }
    
    //这里写int有隐患，但是我不知道什么原理，反正reactor肯定会产生问题
    std::unordered_set<int> _clients;
    std::mutex _mtx;
    std::atomic<uint64_t> _reject_count{0};//暂时不用，压测的时候把他打印出来
    IoThreadPool _Pool; 
}; 