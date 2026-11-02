/**
 * ============================================================================
 *  blocking_queue_test.cpp  ——  BlockingQueue<T> 的单元测试
 * ============================================================================
 *
 * 由 test/test_02_blocking_queue.sh 负责编译和运行。
 *
 * 【为什么用自定义 CHECK 而不用 assert】
 *   assert 在定义 NDEBUG 时（-O2 常见）会被整体删除，测试会静默失效。
 *   自定义 CHECK 无论优化等级都会执行，并且【不中断】—— 一次跑完所有
 *   用例，把失败的都列出来，而不是第一个失败就 abort。
 *
 * 【用例设计原则】
 *   1. 每个用例验一件事，失败时能直接定位
 *   2. 涉及阻塞的用例都用"限时等待"，避免死锁导致测试永久挂死
 *      （外层脚本还有 timeout 兜底）
 *   3. MPMC 用例故意把容量压到 8、消息量拉到 10 万，
 *      强制两个条件变量进入最激烈的等待/唤醒循环
 * ============================================================================
 */

#include "blocking_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_pass = 0;
int g_fail = 0;

using Clock = std::chrono::steady_clock;

/** 单条断言。不中断，只计数，失败信息带用例名。 */
void check(bool ok, const std::string& name) {
    if (ok) {
        ++g_pass;
        std::printf("  [PASS] %s\n", name.c_str());
    } else {
        ++g_fail;
        std::printf("  [FAIL] %s\n", name.c_str());
    }
}

long long ms_since(Clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
}

/**
 * 轮询等待一个 atomic 标志变成 true，最多等 timeout_ms。
 * 返回 false 表示超时 —— 用它代替裸 join()，
 * 这样"应该被唤醒但没被唤醒"的 bug 会报 FAIL 而不是把进程挂死。
 */
bool wait_flag(const std::atomic<bool>& flag, int timeout_ms) {
    auto t0 = Clock::now();
    while (!flag.load()) {
        if (ms_since(t0) > timeout_ms) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return true;
}

// ---------------------------------------------------------------------------
//  用例 1：基本 push / pop，值与返回值都要对
// ---------------------------------------------------------------------------
void test_basic() {
    std::puts("[1] 基本推拉");
    BlockingQueue<int> q(4);
    check(q.push(1), "push 返回 true");
    int v = 0;
    check(q.pop(v) && v == 1, "pop 返回 true 且值正确");
    check(q.size() == 0, "取出后 size() 归零");
}

// ---------------------------------------------------------------------------
//  用例 2：FIFO 顺序
//    队列必须先进先出，否则任务处理顺序错乱（连接会乱序关闭）
// ---------------------------------------------------------------------------
void test_fifo() {
    std::puts("[2] FIFO 顺序");
    BlockingQueue<int> q(8);
    for (int i = 0; i < 5; ++i) (void)q.push(i);

    bool order_ok = true;
    for (int i = 0; i < 5; ++i) {
        int v = -1;
        if (!q.pop(v) || v != i) order_ok = false;
    }
    check(order_ok, "5 个元素按插入顺序取出");
}

// ---------------------------------------------------------------------------
//  用例 3：move-only 类型
//    线程 / unique_ptr 不可拷贝，队列必须能承载它们
//    （这正是 pop 用 std::move 而不是拷贝的根因）
// ---------------------------------------------------------------------------
void test_move_only() {
    std::puts("[3] move-only 类型");
    BlockingQueue<std::unique_ptr<int>> q(4);
    check(q.push(std::make_unique<int>(42)), "push unique_ptr 成功");

    std::unique_ptr<int> p;
    bool popped = q.pop(p);
    check(popped && p != nullptr && *p == 42, "pop 后指针有效且值正确");
}

// ---------------------------------------------------------------------------
//  用例 4：MPMC 压力 —— 最关键的一条
//    容量只有 8，消息有 10 万条，生产者几乎永远卡在 _not_full，
//    4 个消费者几乎永远卡在 _not_empty，两个条件变量被压到极限。
//
//    求和断言 sum == N(N+1)/2 同时覆盖三类 bug：
//      丢消息（谓词写错）    -> sum 偏小
//      重复消费（忘 pop_front）-> sum 偏大
//      死锁                  -> join 不返回（脚本 timeout 兜底）
// ---------------------------------------------------------------------------
void test_mpmc_stress() {
    std::puts("[4] MPMC 压力（4 消费者 / 10 万条 / 容量 8）");
    const int N = 100000;
    BlockingQueue<int> q(8);
    std::atomic<long long> sum{0};

    auto t0 = Clock::now();

    std::vector<std::thread> consumers;
    consumers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        consumers.emplace_back([&q, &sum] {
            int v = 0;
            while (q.pop(v)) sum.fetch_add(v, std::memory_order_relaxed);
        });
    }

    for (int i = 1; i <= N; ++i) (void)q.push(i);
    q.close();                                   // 排空后消费者自然退出

    for (auto& t : consumers) t.join();

    const long long expect = static_cast<long long>(N) * (N + 1) / 2;
    check(sum.load() == expect, "10 万条不丢不重（求和校验）");
    check(q.size() == 0, "结束后队列已排空");
    std::printf("       耗时 %lld ms\n", ms_since(t0));
}

// ---------------------------------------------------------------------------
//  用例 5：close 的语义
//    a) close 后 push 必须失败（拒绝新任务）
//    b) close 后 pop 必须失败（消费者能退出）
//    c) close 可重复调用
// ---------------------------------------------------------------------------
void test_close_semantics() {
    std::puts("[5] close 语义");
    BlockingQueue<int> q(4);
    q.close();

    check(!q.push(1), "close 后 push 返回 false");
    int v = 0;
    check(!q.pop(v), "close 且空队列时 pop 返回 false");
    check(q.closed(), "closed() 返回 true");

    q.close();                                   // 幂等
    check(q.closed(), "close 可重复调用不崩");
}

// ---------------------------------------------------------------------------
//  用例 6：优雅关闭 —— 队列里已有的元素必须能取完
//    close() 只置标志【不清空队列】，这是优雅关闭的基础。
//    若 pop 里先判 _closed 再判 empty，剩下的任务会被全部丢弃。
// ---------------------------------------------------------------------------
void test_graceful_drain() {
    std::puts("[6] 优雅关闭（排空）");
    BlockingQueue<int> q(16);
    for (int i = 0; i < 10; ++i) (void)q.push(i);
    q.close();

    int cnt = 0;
    int v = 0;
    while (q.pop(v)) ++cnt;
    check(cnt == 10, "close 后剩余 10 个元素仍能全部取出");
}

// ---------------------------------------------------------------------------
//  用例 7：消费者阻塞 -> 被生产者唤醒
//    pop 不能返回"没数据"，必须真正睡下；push 到了要立刻醒
// ---------------------------------------------------------------------------
void test_pop_blocks_until_push() {
    std::puts("[7] 空队列 pop 阻塞，push 唤醒");
    BlockingQueue<int> q(4);
    std::atomic<bool> done{false};
    std::atomic<int> got{0};

    std::thread consumer([&q, &done, &got] {
        int v = 0;
        if (q.pop(v)) got.store(v);
        done.store(true);
    });

    // 先确认它确实阻塞住了（150ms 内不该完成）
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    check(!done.load(), "空队列时 pop 阻塞住（没有忙等返回）");

    (void)q.push(777);
    check(wait_flag(done, 1000), "push 后被唤醒");
    check(got.load() == 777, "唤醒后拿到正确值");
    consumer.join();
}

// ---------------------------------------------------------------------------
//  用例 8：生产者阻塞 -> 队满时不能丢数据
//    有界队列的核心价值：满就阻塞，让上游降速，而不是丢弃任务
// ---------------------------------------------------------------------------
void test_push_blocks_when_full() {
    std::puts("[8] 队满 push 阻塞，pop 唤醒");
    BlockingQueue<int> q(1);
    (void)q.push(1);                             // 填满

    std::atomic<bool> done{false};
    std::atomic<bool> ret{true};
    std::thread producer([&q, &done, &ret] {
        ret.store(q.push(2));                    // 应阻塞
        done.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    check(!done.load(), "队满时 push 阻塞住（没有丢弃）");

    int v = 0;
    check(q.pop(v) && v == 1, "pop 取出第一个元素");
    check(wait_flag(done, 1000), "pop 后生产者被唤醒");
    check(ret.load(), "push 最终返回 true");

    int v2 = 0;
    check(q.pop(v2) && v2 == 2, "第二个元素没丢");
    producer.join();
}

// ---------------------------------------------------------------------------
//  用例 9：卡在 push 上的生产者，必须能被 close() 解除阻塞
//    ★ 最容易漏的一条：队列满 -> 生产者睡在 _not_full 上；
//      如果 close 只 notify _not_empty，这个生产者会【永远卡死】，
//      shutdown 时 join 不返回，进程退不出去。
// ---------------------------------------------------------------------------
void test_close_unblocks_full_producer() {
    std::puts("[9] close 解除生产者的阻塞");
    BlockingQueue<int> q(1);
    (void)q.push(1);                             // 填满

    std::atomic<bool> done{false};
    std::atomic<bool> ret{true};
    std::thread producer([&q, &done, &ret] {
        ret.store(q.push(2));                    // 卡在 _not_full
        done.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    check(!done.load(), "生产者确实阻塞在 push 上");

    q.close();                                   // 必须同时 notify _not_full
    check(wait_flag(done, 1000), "close 后生产者被唤醒（没有永久卡死）");
    check(!ret.load(), "close 后 push 返回 false");
    producer.join();
}

// ---------------------------------------------------------------------------
//  用例 10：构造参数校验 —— cap==0 必须抛异常而不是死锁
// ---------------------------------------------------------------------------
void test_capacity_guard() {
    std::puts("[10] 容量校验");

    bool thrown = false;
    try {
        BlockingQueue<int> bad(0);
        (void)bad;
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    check(thrown, "cap==0 抛 std::invalid_argument");

    BlockingQueue<int> def;
    check(def.capacity() == 1024, "默认容量 1024");

    BlockingQueue<int> custom(64);
    check(custom.capacity() == 64, "自定义容量 64");
}

}  // namespace

int main() {
    std::puts("================ BlockingQueue 单元测试 ================");

    test_basic();
    test_fifo();
    test_move_only();
    test_mpmc_stress();
    test_close_semantics();
    test_graceful_drain();
    test_pop_blocks_until_push();
    test_push_blocks_when_full();
    test_close_unblocks_full_producer();
    test_capacity_guard();

    std::printf("========================================================\n");
    std::printf("  PASS: %d    FAIL: %d\n", g_pass, g_fail);
    std::printf("========================================================\n");

    return g_fail == 0 ? 0 : 1;
}