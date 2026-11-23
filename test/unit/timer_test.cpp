/**
 * test/unit/timer_test.cpp —— 定时器纯内存单元测试（不碰 socket、不睡觉）
 *
 * 编译运行（不需要 cmake）：
 *   g++ -std=c++17 -Wall -Wextra -Werror -Ilib -Isrc/timer \
 *       test/unit/timer_test.cpp -o /tmp/timer_test && /tmp/timer_test
 *
 * 【本文件的铁律】绝不 sleep。
 * 定时器的正确性和「真实流逝了多少时间」无关，只和「你喂进去的 now」有关。
 * 一旦用了 sleep_for，测试就是慢的、抖的、在 CI 上偶发失败的。
 * 所以所有用例都自己构造 TimerPoint，用 TimerClock::now() + N ms 手工拨快。
 * 只有最后一个 case（和真实时钟对齐）才用 now()，且不做时间断言。
 */

#include "scan_timer.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

// ============================ 测试基础设施 ============================
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (cond) {                                                          \
            ++g_pass;                                                        \
        } else {                                                             \
            ++g_fail;                                                        \
            std::printf("  [FAIL] %s   (line %d)\n", #cond, __LINE__);       \
        }                                                                    \
    } while (0)

#define CHECK_EQ(a, b)                                                       \
    do {                                                                     \
        auto va = (a);                                                       \
        auto vb = (b);                                                       \
        if (va == vb) {                                                      \
            ++g_pass;                                                        \
        } else {                                                             \
            ++g_fail;                                                        \
            std::printf("  [FAIL] %s == %s   (line %d)\n", #a, #b, __LINE__);\
        }                                                                    \
    } while (0)

// 测试基准时刻：全部用例都相对它偏移，保证可重复
static TimerPoint base() {
    // 用固定偏移而不是 now()，让所有用例的时刻完全确定
    return TimerClock::now();
}

static TimerPoint at_ms(int64_t ms) {
    return base() + std::chrono::milliseconds(ms);
}

// ============================ 用例 ============================

/**
 * 1) 基本到期：add 3 条，拨到中间时刻只有该到期的触发
 */
static void test_basic_expire() {
    std::printf("[1] 基本到期\n");
    ScanTimer t;
    t.add(1, at_ms(100));
    t.add(2, at_ms(200));
    t.add(3, at_ms(300));
    CHECK_EQ(t.size(), 3u);

    std::vector<int> fired;
    auto n = t.advance(at_ms(150), [&](int id) { fired.push_back(id); });
    CHECK_EQ(n, 1u);
    CHECK_EQ(fired.size(), 1u);
    CHECK(fired.size() == 1 && fired[0] == 1);
    CHECK_EQ(t.size(), 2u);   // 1 已摘除，2/3 还在

    fired.clear();
    n = t.advance(at_ms(150), [&](int id) { fired.push_back(id); });
    CHECK_EQ(n, 0u);          // 同一时刻再扫一次，没有新到期
    CHECK_EQ(t.size(), 2u);
}

/**
 * 2) 同 id 重复 add = 刷新，不是新增
 *    这是 idle_scanner 每 recv 一次就 add 一次的前提
 */
static void test_refresh_same_id() {
    std::printf("[2] 同 id 刷新\n");
    ScanTimer t;
    t.add(7, at_ms(100));
    t.add(7, at_ms(500));     // 刷新到更晚
    CHECK_EQ(t.size(), 1u);   // ★ 必须还是 1，不能变 2

    std::vector<int> fired;
    t.advance(at_ms(200), [&](int id) { fired.push_back(id); });
    CHECK_EQ(fired.size(), 0u);  // 原定 100ms 到期，但已刷到 500ms

    t.advance(at_ms(600), [&](int id) { fired.push_back(id); });
    CHECK(fired.size() == 1 && fired[0] == 7);
    CHECK_EQ(t.size(), 0u);
}

/**
 * 3) cancel：取消后不再触发，返回值区分「存在/不存在」
 */
static void test_cancel() {
    std::printf("[3] cancel\n");
    ScanTimer t;
    t.add(1, at_ms(100));
    t.add(2, at_ms(100));

    CHECK(t.cancel(1));        // 存在 → true
    CHECK(!t.cancel(99));      // 不存在 → false
    CHECK_EQ(t.size(), 1u);

    std::vector<int> fired;
    t.advance(at_ms(200), [&](int id) { fired.push_back(id); });
    CHECK(fired.size() == 1 && fired[0] == 2);
}

/**
 * 4) 回调里 add 同一个 id —— 本轮绝不能重复触发（两阶段摘除的意义）
 *    如果实现是「边遍历边回调」，这里会崩或者重复触发
 */
static void test_reentrant_add() {
    std::printf("[4] 回调内 re-add 同 id\n");
    ScanTimer t;
    t.add(5, at_ms(100));

    int count = 0;
    t.advance(at_ms(200), [&](int id) {
        ++count;
        t.add(id, at_ms(1000));   // 在回调里给同一个 id 续命
    });
    CHECK_EQ(count, 1);           // ★ 本轮只触发一次，不能因 re-add 又触发
    CHECK_EQ(t.size(), 1u);       // 新记录已存在

    // 拨到新时刻，应该再触发一次
    count = 0;
    t.advance(at_ms(1100), [&](int) { ++count; });
    CHECK_EQ(count, 1);
}

/**
 * 5) 回调里 cancel 另一个 id —— 本轮它仍会被回调吗？
 *    契约：阶段 2 已把到期项全部摘除，所以 cb 只作用于本批次。
 *    这里验证 cancel 不会破坏遍历。
 */
static void test_reentrant_cancel() {
    std::printf("[5] 回调内 cancel 其他 id\n");
    ScanTimer t;
    t.add(1, at_ms(100));
    t.add(2, at_ms(100));

    std::vector<int> fired;
    t.advance(at_ms(200), [&](int id) {
        fired.push_back(id);
        t.cancel(2);              // 2 本轮也已到期，摘除发生在回调之前
    });
    CHECK_EQ(fired.size(), 2u);   // 两条都该触发（摘除阶段已完成）
    CHECK_EQ(t.size(), 0u);
}

/**
 * 6) 大批量：10000 条里只有一半到期，验证不漏不重
 */
static void test_bulk() {
    std::printf("[6] 批量 10000\n");
    ScanTimer t;
    for (int i = 0; i < 10000; ++i) {
        t.add(i, at_ms(i % 2 == 0 ? 100 : 200));   // 偶数 100ms，奇数 200ms
    }
    CHECK_EQ(t.size(), 10000u);

    std::size_t even = 0, odd = 0;
    t.advance(at_ms(150), [&](int id) {
        if (id % 2 == 0) ++even; else ++odd;
    });
    CHECK_EQ(even, 5000u);
    CHECK_EQ(odd, 0u);
    CHECK_EQ(t.size(), 5000u);

    t.advance(at_ms(250), [&](int id) {
        if (id % 2 == 0) ++even; else ++odd;
    });
    CHECK_EQ(odd, 5000u);
    CHECK_EQ(even, 5000u);   // 偶数不应再触发
    CHECK_EQ(t.size(), 0u);
}

/**
 * 7) 空表 advance：不应有开销，也不应崩
 */
static void test_empty() {
    std::printf("[7] 空表\n");
    ScanTimer t;
    CHECK_EQ(t.size(), 0u);
    CHECK_EQ(t.advance(at_ms(999999), [](int) {}), 0u);
    CHECK_EQ(t.size(), 0u);
}

/**
 * 8) 边界：deadline 恰好等于 now 必须触发（用 <= 而不是 <）
 *    06_timer.sh 阶段 [4]（0.75× 间隔）抓的就是这类边界
 */
static void test_exact_boundary() {
    std::printf("[8] deadline == now 边界\n");
    ScanTimer t;
    t.add(1, at_ms(100));

    int count = 0;
    t.advance(at_ms(99), [&](int) { ++count; });
    CHECK_EQ(count, 0);       // 差 1ms 不该触发

    t.advance(at_ms(100), [&](int) { ++count; });
    CHECK_EQ(count, 1);       // 恰好到期必须触发
}

/**
 * 9) 接口自述：name()
 */
static void test_name() {
    std::printf("[9] name()\n");
    ScanTimer t;
    CHECK(std::string(t.name()) == "scan");
}

// ============================ main ============================
int main() {
    std::printf("==== timer_test (ScanTimer) ====\n");
    test_basic_expire();
    test_refresh_same_id();
    test_cancel();
    test_reentrant_add();
    test_reentrant_cancel();
    test_bulk();
    test_empty();
    test_exact_boundary();
    test_name();

    std::printf("\n---- pass=%d fail=%d ----\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}