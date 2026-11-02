/**
 * ============================================================================
 *  ThreadPerConnServer  ——  「一连接一线程」模型
 * ============================================================================
 *
 * 【在继承体系里的位置】
 *   TcpServerBase        模板方法基类：把 socket 的固定流程写死
 *                        start() = create_socket -> set_reuse_addr
 *                                   -> do_bind -> do_listen -> _running=true -> run_loop()
 *                        其中 run_loop() 是纯虚函数，留给子类决定「怎么并发」
 *
 *   ThreadPerConnServer  本文件。run_loop() 的实现方式是：
 *                        accept 成功一次，就 new 一个线程去服务这条连接。
 *                        同时 fork_server.hpp / IO_server.hpp 会是另外两种实现。
 *
 * 【本文件负责的两件事】
 *
 *   1) run_loop()            accept 循环 + 错误分类调度（只做「派活」，不做业务）
 *        不直接调用 accept()，而是调基类的 accept_once()，它只返回一个 int 并保留 errno。
 *        把「int 返回值 + errno」翻译成 AcceptOutCome 这件事，放在本文件的 run_loop 里做：
 *            struct AcceptOutCome { AcceptStatus status; int fd; int err; };   // 基类只声明类型
 *        分层理由：这张 errno 表是「策略」，会随并发模型变化（比如线程池多了背压分支）；
 *                  而基类只该提供「无状态原语」accept_once()。所以翻译留在子类，
 *                  基类就不需要为了换并发模型而修改 —— 基类只留一个虚函数 run_loop()。
 *
 *        五个分支的语义：
 *            Accept    —— 成功。重置退避、清零计数、打恢复日志、起线程
 *            Backoff   —— EMFILE/ENFILE（fd 满）。腾 fd + 退避 + 节流日志
 *            RetryNow  —— EINTR/ECONNABORTED。立刻 continue，不睡
 *            Stopped   —— EBADF/EINVAL 或 !_running。return 退出整个循环
 *            Fatal     —— 其他未知错误。退避 + 节流日志（当前缺连续次数上限）
 *
 *        三条贯穿全局的「策略状态」：
 *            backoff   指数退避时长 100 -> 200 -> 400 -> 800ms（封顶），成功即重置
 *            err_count 连续失败次数，恢复时用它打一条汇总日志
 *            last_log  节流时间戳，保证「同一类日志 1 秒最多 1 条」
 *
 *        为什么策略写在这里而不在 accept_once() 里：
 *            这三个状态必须跨多次调用存活，只有「调用方」才有这个生命周期。
 *            accept_once() 是无状态原语，谁调都一样，所以它能被所有子类复用。
 *
 *   2) client_thread_func()  单条连接的生命周期，跑在独立线程上
 *            recv 循环 -> n>0 交给 _msg_cb 业务回调
 *                      -> n==0 对端 FIN，优雅关闭，break
 *                      -> n<0  EINTR 重试 / 其他错误 break
 *            最后 close(fd)。
 *
 *            注意 recv 用 flags=0 而不是 MSG_PEEK：MSG_PEEK 只窥探不消费，
 *            同一份数据会被无限次读到，表现为 CPU 100% 且客户端毫无响应。
 *
 * 【本模型的已知不足】（详见 note/note_stage2_accept错误处理与fd耗尽.md）
 *   - 线程数无上限：accept 成功就无条件 new 线程，并发度由客户端决定。
 *     1 万连接 ≈ 80GB 虚拟栈 + 160~320MB 内核内存，且无任何准入控制（背压）。
 *   - detach() + 捕获 this：线程拿 this 去访问 _running / _msg_cb，而 server 通常是
 *     栈对象。start() 返回后对象析构，worker 仍在跑 -> UAF。当前「能工作」只是因为
 *     run_loop() 不返回，属于运气。
 *   - stop() 唤不醒阻塞在 recv 的 worker：_running=false 只是内存标记，内核不看它。
 *     close(fd) 也不行（file 对象引用还在），必须 shutdown(fd, SHUT_RDWR)；
 *     但 fd 只活在 client_thread_func 的形参里，主线程拿不到这个集合 -> 死结。
 *   - 无法优雅关闭：没有线程句柄，不能 join，不知道还剩几个 worker。
 *
 * 以上四点的解法是「线程池 + 有界阻塞队列」：把连接从「每个线程的私有栈变量」
 * 变成「队列里可追踪的任务」，从而让 shutdown / 限流 / join 都变得可实现。
 * ============================================================================
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

class ThreadPerConnServer : public TcpServerBase{
public:
    using TcpServerBase::TcpServerBase; //继承构造函数
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
                //这里不建议使用RAII的 my——thread ，因为函数用到了this指针，fd安全因为是按值捕获
                // 如果线程走完后this已经释放，那么mythread析构时join（）后会发现没有this因此也就会出现无定义情况
                //如果走detach后台的话，mythread和std：：thread没区别
                std::thread t([this,fd = res_fd.fd](){
                    client_thread_func(fd);
                });
                t.detach();
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
                return;
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
    }

private:
    void client_thread_func(int client_fd){
        char buf[4096];
        while (_running) {
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
        close(client_fd);
    }
}; 