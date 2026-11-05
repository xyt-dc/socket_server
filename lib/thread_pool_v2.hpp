//
// 先把概念理清，这个模式叫线程池➕任务队列，线程池是一个vector，任务队列是一个队列（可以是有限可以是普通的queue，dequeue）
//数据结构主要就是线程池vector+队列queue，由于queue和线程池都是共享资源所以要加锁，另外涉及生产者消费者模式
//所以我们加入条件变量来唤醒worker
//

#pragma once 

#include <cstdio>
#include <thread>
#include <queue>
#include <mutex>
#include <functional>
#include <future>
#include <atomic>

class ThreadPool_v2 {
public:
    explicit ThreadPool_v2(size_t Thread_counts):_stop(false){
        //默认+代参
        for (size_t i = 0;i<Thread_counts;++i) {
            _workers.emplace_back([this](){
                for(;;){
                    //取出并消费task
                    std::function<void()> task;
                    std::unique_lock<std::mutex> lk(_mtx);
                    this->_cond.wait(lk,[&](){
                        //醒来条件 1.我现在有活要干 2.我下班了
                        return !_tasks.empty() || _stop;
                    });
                    if (_tasks.empty()){
                        // 没活干但是也醒了说明stop信号来了要下班辣
                        return;
                    }
                    task = std::move(_tasks.front());
                    _tasks.pop();
                    lk.unlock();
                    try{
                        task();
                    }catch(...){
                        //日志记录即可
                    }
                }
            });
        }
    }

    template<class F,class... Args>
    auto enqueue(F&& f,Args&&... args) -> std::future<typename std::invoke_result<F,Args...>::type>{
        //完美转发
        using res_type = typename std::invoke_result<F,Args...>::type;

        //生产task
        auto task = std::make_shared<std::packaged_task<res_type()>>(
            [f = std::forward<F>(f),
             tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
                return std::apply(std::move(f), std::move(tup));
            }
        );
        std::future<res_type> res = task->get_future();
        std::unique_lock<std::mutex> lk(_mtx);
        if(_stop){
            throw std::runtime_error("work out ,stop push ,system will stop while all of tasks in queue work out");
        }
        _tasks.emplace([task](){
            (*task)();
        });
        lk.unlock();
        _cond.notify_one();
        return res;
    }

    ~ThreadPool_v2() {
        //逻辑就是：通知stop，然后叫醒所有工人，然后遍历让手里有活的工人把手头的工作干完
        _stop = true;
        _cond.notify_all();
        for(auto& worker : _workers){
            worker.join();
        }
    }

private:
    //任务队列的核心，这里不能直接把函数范型成T加进去，因为T只能指代一个类型，不能指代函数
    //必须用function打包,在并行编程领域尽量不用bind会引发竞争或死锁问题，尽量用lambda
    std::queue<std::function<void()>> _tasks;
    std::mutex _mtx;

    //线程池的核心
    std::vector<std::thread> _workers;//工人
    std::condition_variable _cond;//叫醒机制
    std::atomic<bool> _stop;//上下班通知
};


