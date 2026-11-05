#include "thread_pool_v2.hpp"
#include <iostream>
#include <chrono>

/**
 * 编译运行命令
 * g++ -std=c++17 -O2 -Wall -Wextra -I../lib thread_pool_test.cpp -o pooltest -pthread
 * ./pooltest
 * # 调试版：带断言、能做 gdb :g++ -std=c++17 -O0 -g -Wall -Wextra -I../lib thread_pool_test.cpp -o pooltest_O0 -pthread
 * 
 * 
 * 
 */

long long cpu_task(int n, int seed) {
    long long sum = seed;
    for (int i = 0; i < n; ++i) {
        sum = (sum * 31 + i) % 1000000007;  // 依赖前一步结果，无法向量化/折叠
    }
    return sum;
}

int main(){
    
    const size_t thread_count = std::thread::hardware_concurrency();
    const int task_count = 100000;   // 10 万个任务
    const int work_load = 2000;      // 每个任务循环 2000 次

    std::cout << "硬件线程数: " << thread_count << "\n";
    std::cout << "任务总数:   " << task_count << "\n\n";

    ThreadPool_v2 pool(thread_count);
    // 注意：这里必须是 reserve 而不是 vector(task_count)。
    // vector(task_count) 会先构造 task_count 个【无效】future，
    // 后面 push_back 又把新 future 追加到末尾，于是总共 2*task_count 个元素，
    // 前 task_count 个是默认构造的、没有关联状态，
    // 下面 f.get() 一调用就抛 std::future_error: No associated state。
    std::vector<std::future<long long>> futures;
    futures.reserve(task_count);

    auto t0 = std::chrono::steady_clock::now();
    for(int i=0;i<task_count;++i){
        futures.push_back(pool.enqueue(cpu_task,work_load,i));
    }
    auto t1 = std::chrono::steady_clock::now();

    long long checksum = 0;
    for(auto& f :futures){
        checksum += f.get();
    }
    auto t2 = std::chrono::steady_clock::now();

    auto t3 = std::chrono::steady_clock::now();
    long long checksum_single = 0;
    for(int i=0;i<task_count;++i){
        checksum_single += cpu_task(work_load,i);
    }
    auto t4 = std::chrono::steady_clock::now();
    auto ms = [](auto a,auto b){
        return std::chrono::duration_cast<std::chrono::milliseconds>(b-a).count();
    };
    std::cout << "提交耗时:     " << ms(t0, t1) << " ms\n";
    std::cout << "执行+收集:    " << ms(t1, t2) << " ms\n";
    std::cout << "线程池总耗时: " << ms(t0, t2) << " ms\n";
    std::cout << "单线程耗时:   " << ms(t3, t4) << " ms\n";
    std::cout << "加速比:       "
              << static_cast<double>(ms(t3, t4)) / ms(t0, t2) << "x\n";
    std::cout << "结果校验:     "
              << (checksum == checksum_single ? "一致 PASS" : "不一致 FAIL")
              << "\n";

    return 0;
}