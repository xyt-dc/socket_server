/**
 * 服务端专用线程池
 *
 * 和 Classical_ThreadPool 的区别：
 *   1. 无返回值 —— 用 std::function<void()>，不要 future/packaged_task
 *   2. 有界队列 —— 复用 BlockingQueue，满了不 OOM，而是让调用方知道
 *   3. submit 返回 bool —— 满了/停了都返回 false，调用方（accept 处）自己决定丢连接
 *   4. 异常全吞 —— catch(...)，一个连接的处理代码抛了不能影响别的连接
 */
#pragma once

#include "blocking_queue.hpp"
#include <atomic>
#include <cstddef>
#include <functional>
#include <thread>
#include <vector>
#include <iostream>

class IoThreadPool {
public:
    using Task = std::function<void()>;

    // n = worker 线程数，queue_cap = 待办队列上限
    explicit IoThreadPool(size_t n, size_t queue_cap = 1024):_tasks(queue_cap) { 
         for (size_t i = 0; i < n; i++)
         {
            //mutable 是给"要修改按值捕获的变量"用的
            _workers.emplace_back([this](){
                worker_loop();
            });
         }
    }

    ~IoThreadPool() { 
        shutdown();
     }

    // 禁止拷贝：线程和队列都不能复制
    IoThreadPool(const IoThreadPool&) = delete;
    IoThreadPool& operator=(const IoThreadPool&) = delete;

    // 提交任务。返回 false = 队列满 或 池子已关，任务没被接受
    template <typename F>
    bool submit(F&& f) { 
        //「队列或池子满不可以退避休息再重连吗？」可以，但那是调用方的事，不是线程池的事。 这是设计分层的关键。
        return _tasks.try_push(Task(std::forward<F>(f)));
     }

    // 优雅关闭：不再接受新任务，但已入队的任务全部执行完
    // 幂等 + 线程安全：多个线程同时调用只会真正执行一次
    void shutdown() {
        std::call_once(_once, [this] {
            _tasks.close();                                  // 1. 叫醒所有阻塞者
            for (auto& w : _workers) {
                if (w.joinable()) w.join();                   // 2. 等 worker 排空退出
            }
            _workers.clear();                                 // 3. 清掉线程对象
        });
    }

    // ★ 调用方契约：必须在 IoThreadPool 析构【之前】停止调用本函数。
//   pool 内部保证 _tasks 比 _workers 活得久，所以只要对象还在，
//   和 shutdown() 并发调用是安全的（close 后非空队列仍会被执行，这是排空语义）。
//   但 shuttle 本身的生命周期由调用方负责 —— 析构后再调 = this 悬空。
//
// ★ 架构约束：本函数会让【调用者线程】执行池里的任意任务。
//   只有当池里全是"短任务"时才安全；混进一个阻塞任务（比如
//   while(true) recv 的 handle_client_read），reactor 线程就会被卡死，
//   整个服务器停止 accept。
//   而"任务短"恰恰是 epoll 提供的 —— 这两个东西必须配对使用。
    bool try_run_one(){
        Task out;
        auto ok = _tasks.try_pop(out);
        if (!ok){
            return false;
        }
        try{
            out();
        }catch(const std::exception& e){
            std::cerr << e.what() << '\n';
        }catch (...) {
            std::cerr << "task threw unknown\n";
        }
        return true;
    }

    size_t worker_count() const { return _workers.size(); }
    size_t queue_size()   const { return _tasks.size(); }
    size_t queue_cap()    const { return _tasks.capacity(); }

private:
    // worker 线程的一生：循环 pop 任务并执行
    void worker_loop() {
        while (true)
        {
            //调用blocking_queue
            Task out;
            if(_tasks.pop(out)){
                //！！！又忘了try
                try{
                     out();
                }catch(const std::exception& e){
                    std::cerr << e.what() << '\n';
                }catch (...) {
                    std::cerr << "task threw unknown\n";
                }   
            }else
            {
                return; //下班了
            }
        }
        
    }

    // ★ 声明顺序：_tasks 必须在 _workers 之前
    BlockingQueue<Task> _tasks;
    std::vector<std::thread> _workers;
    std::once_flag _once;      // ← 新增
};

