/**
线程池的实现，独立的，可以共用的线程池
当前代码实现的是简单版本的线程池
上面的实现是固定线程数的版本，实际工程里往往还需要：

动态扩容/缩容：任务积压时加线程，空闲时回收，避免线程数长期虚高
任务优先级：把 std::queue 换成 std::priority_queue，按优先级取任务
异常安全：task() 外层包 try/catch，避免单个任务抛异常杀死整个工作线程
线程数选择：CPU 密集型取 hardware_concurrency()，I/O 密集型可适当加倍，因为等待 I/O 时不占 CPU
*/
#pragma once

#include "blocking_queue.hpp"
#include "jthread.hpp"
#include <functional>
#include <vector>
#include <queue>
#include <mutex>
#include <future>
#include <stdexcept>
#include <memory>
#include <tuple>
#include <iostream>

class Classical_ThreadPool{
public:

    explicit Classical_ThreadPool(size_t threads):_stop(false){
        for (size_t i = 0; i < threads; i++)
        {
            _workers.emplace_back([&](){
                for(;;){
                    std::function<void()> task;
                    {
                        //持锁等待：直到有任务或者线程池被要求退出
                        std::unique_lock<std::mutex> lk(_mtx);
                        _condition.wait(lk,[&](){
                            return _stop || !_tasks.empty();
                        });
                        if(_tasks.empty()){
                            //如果tasks空那么走到这一步必定是stop了
                            return ;
                        }
                        task = std::move(_tasks.front());
                        _tasks.pop();
                    }
                    //🔒外执行
                    // 异常绝对不能穿出线程入口函数，否则 std::terminate() 整个进程死
                    // 注意 catch(...) 而不是 catch(const std::exception&)：
                    //   throw 42; / throw "x"; 这类非 std::exception 派生的东西
                    //   用 catch(const std::exception&) 抓不住，照样 terminate
                    try
                    {
                        task();
                    }
                    catch(...)
                    {
                        // 离线任务的异常可以吞；服务端里应该记日志
                    }
                }
            });
        }
    }
    /**
        普通参数转发会丢失值类别，导致本来能触发移动语义的右值被当成左值处理，从而多一次拷贝。
        如果不用完美转发，中间函数里的参数不管传进来的是左值还是右值，都会变成左值，
        目标函数只能按左值重载处理。完美转发能保留“原样”，让调用者传左值时目标函数按左值接收，
        传右值时目标函数按右值接收，常用于工厂函数、包装器和容器 emplace 系列
        标准写法是模板参数用 T&&（万能引用），转发时用 std::forward<T>(arg)
    */
    template<class F, class... Args>
    auto enqueue(F&& f,Args&&... args)->std::future<typename std::invoke_result<F,Args...>::type>{
        using return_type = typename std::invoke_result<F,Args...>::type;
        /**
         * 【为什么不能用 std::bind】
         * bind 内部把参数 decay 之后按【值】存进一个 tuple，所以：
         *   1. 外面写的 std::forward<Args> 白费，一进 bind 就退化成拷贝
         *   2. std::unique_ptr 这类只可移动的参数根本传不进去（要拷贝 → 编译失败）
         *
         * 【为什么不能用 C++20 的包捕获 ...args】
         *   [f = ..., ...args = std::forward<Args>(args)] 是 C++20 语法，
         *   本项目锁 C++17，g++ 会报 -Wc++20-extensions（或直接 error）。
         *   而 [args = std::forward<Args>(args)...] 这个写法【任何标准都非法】，
         *   因为捕获列表里 ... 不能放在后面展开。
         *
         * 【C++17 的正确做法：tuple 打包 + std::apply 展开】
         *   - 进 lambda 之前先用 make_tuple 把参数【移动】进 tuple
         *   - lambda 里用 std::apply 把 tuple 拆开喂给 f
         *   - 全程都是移动，unique_ptr 也能过
         */
        //单线程多链接有三种解决方案：async、packaged—task + get-future（）、promise
        auto task = std::make_shared<std::packaged_task<return_type()>>(
            [f = std::forward<F>(f),
             tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
                return std::apply(std::move(f), std::move(tup));
            }
        );
        std::future <return_type> res = task->get_future();

        {
            std::unique_lock<std::mutex> lk(_mtx);
            if(_stop){
                throw std::runtime_error("enqueue on stopped ThreadPool");
            }
            _tasks.emplace([task](){
                (*task)();
            });
        }
        _condition.notify_one();
        return res;
    }
    ~Classical_ThreadPool(){
        {
            std::unique_lock<std::mutex> lock(_mtx);
            _stop = true;
        }
        _condition.notify_all();
        for (auto& worker : _workers){
            worker.join();
        }
    }
private:
    std::queue <std::function<void()>> _tasks;
    std::vector <std::thread> _workers;
    std::mutex _mtx;
    std::condition_variable _condition;
    bool _stop;
};

/**
 * 【我原来的理解】—— 打 ❌ 的地方是我后来验证出来错的
 *
 * ✅ 需要的东西：任务队列 + worker 列表 + 互斥锁 + 条件变量 + stop 标志。这个没错。
 *
 * ❌ "构造函数相当于主线程，完成抢锁➕任务出栈和执行的工作"
 *    错。构造函数【只负责创建线程】，创建完立刻返回，它自己不干活。
 *    _workers.emplace_back(lambda) 的语义是"把这个 lambda 交给一个新的 OS 线程"
 *    然后【马上返回】，不是"调用这个 lambda"。
 *    验证：在构造函数里 emplace_back 之后打一行 printf，会发现这行
 *    【比】worker 里的第一行输出先打印出来。
 *    真正干活的是每个 worker 线程自己跑的那个 for(;;) 循环。
 *
 * ✅ for(;;) 在干什么：worker 的一生。循环体 = 抢锁 → 等条件 → 取任务 → 解锁 → 执行。
 *    一直循环到「队列已 stop 且已排空」才 return 退出。
 *
 * ✅ enqueue：把任务塞进队列，然后 notify_one 叫醒【一个】worker。
 *
 * ❌ "stop家空队列" —— 是"排空"不是"清空"。
 *    析构里的顺序是：置 _stop = true → notify_all → 逐个 join。
 *    这里【没有任何一个地方清空过队列】。worker 醒来后先检查
 *    if(_tasks.empty()) 才 return；队列里还有任务的话它会继续 pop 继续执行。
 *    所以是【排空（drain）】，不是【清空（discard）】——
 *    已提交的任务一个都不会丢，这是优雅退出的核心语义。
 *
 * ✅ jthread：确实用不上。std::thread 需要手动 join，my_jthread 用 RAII 自动 join，
 *    但这个池子的析构函数里已经显式 join 了，再加一层包装没有收益。
 *
 * 【生命周期要点】
 *   成员声明顺序决定了析构顺序（倒序）：
 *     声明 _tasks 先、_workers 后  →  析构时 _workers 先、_tasks 后
 *   这样即使哪天忘了在析构里 join，std::thread 析构时 joinable() 仍为 true
 *   会 std::terminate()，也就是"线程先死"，不会出现 worker 在已销毁的
 *   _tasks 上调 pop() 的 use-after-free。
 *   这是零成本的安全网，纯靠声明顺序。
 */

 