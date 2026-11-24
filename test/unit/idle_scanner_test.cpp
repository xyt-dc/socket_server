/**
 * test/unit/idle_scanner_test.cpp —— IdleScanner 纯内存单元测试
 *
 * 编译运行（不需要 cmake）：
 *   g++ -std=c++17 -Wall -Wextra -Werror -Isrc/timer \
 *       test/unit/idle_scanner_test.cpp -o /tmp/idle_scanner_test && /tmp/idle_scanner_test
 *
 * 【铁律】绝不 sleep。所有用例都自己构造 TimerPoint。
 */

#include "idle_scanner.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

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

using namespace std::chrono;

static TimerPoint T0() { return TimerClock::now(); }

// T0 + N ms
static TimerPoint at(TimerPoint base, int64_t ms) { return base + milliseconds(ms); }

// ---------------------------------------------------------------------------
// [1] touch 之后不 collect，未到期不应报
// ---------------------------------------------------------------------------
static void test_no_expire_before_deadline() {
    std::puts("[1] 未到期不报");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(2000));

    s.touch_at(7, base);
    CHECK_EQ(s.size(), 1u);

    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 1999), fired), 0u);
    CHECK(fired.empty());
    CHECK_EQ(s.size(), 1u);   // 没到期，记录还在

    CHECK_EQ(s.collect(at(base, 2000), fired), 1u);   // 边界 <= 命中
    CHECK_EQ(fired.size(), 1u);
    CHECK_EQ(fired[0], 7);
    CHECK_EQ(s.size(), 0u);
}

// ---------------------------------------------------------------------------
// [2] touch 两次 = 刷新，只有一条记录，deadline 用最后一次
// ---------------------------------------------------------------------------
static void test_touch_refreshes() {
    std::puts("[2] touch 是刷新不是新增");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(1000));

    s.touch_at(3, base);                 // 到期 = base+1000
    s.touch_at(3, at(base, 500));        // 刷新后到期 = base+1500
    CHECK_EQ(s.size(), 1u);              // ★ 关键：不能变成 2 条

    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 1400), fired), 0u);  // 旧 deadline 已被覆盖
    CHECK_EQ(s.collect(at(base, 1500), fired), 1u);
    CHECK_EQ(fired.size(), 1u);
}

// ---------------------------------------------------------------------------
// [3] collect 过的 id 不会再被报第二次
// ---------------------------------------------------------------------------
static void test_collect_is_one_shot() {
    std::puts("[3] collect 只报一次");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(100));

    s.touch_at(11, base);
    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 100), fired), 1u);
    CHECK_EQ(s.collect(at(base, 200), fired), 0u);   // 已经摘掉了
    CHECK_EQ(fired.size(), 1u);
}

// ---------------------------------------------------------------------------
// [4] remove 之后不再报（连接正常关闭的场景）
// ---------------------------------------------------------------------------
static void test_remove() {
    std::puts("[4] remove 后不再报");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(100));

    s.touch_at(5, base);
    CHECK(s.remove(5));          // 原来存在
    CHECK(!s.remove(5));         // 第二次返回 false
    CHECK_EQ(s.size(), 0u);

    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 1000), fired), 0u);
    CHECK(fired.empty());
}

// ---------------------------------------------------------------------------
// [5] timeout == 0 = 功能关闭：touch/collect 全是 no-op
// ---------------------------------------------------------------------------
static void test_disabled() {
    std::puts("[5] timeout=0 表示关闭");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(0));

    CHECK(!s.enabled());
    s.touch_at(1, base);
    CHECK_EQ(s.size(), 0u);      // 一条都没进

    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 100000), fired), 0u);
    CHECK(fired.empty());
    CHECK(!s.remove(1));
}

// ---------------------------------------------------------------------------
// [6] 关闭期间残留 → 重新开启必须清空，不能「刚开启就批量踢人」
// ---------------------------------------------------------------------------
static void test_reenable_clears_stale() {
    std::puts("[6] 关闭再开启要清残留");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(1000));

    s.touch_at(1, base);
    s.touch_at(2, base);
    CHECK_EQ(s.size(), 2u);

    s.set_timeout(milliseconds(0));      // 关掉 → 内部 reset
    CHECK(!s.enabled());
    CHECK_EQ(s.size(), 0u);              // ★ 残留清掉了

    s.set_timeout(milliseconds(1000));   // 再开
    CHECK(s.enabled());
    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 5000), fired), 0u);
    CHECK(fired.empty());
}

// ---------------------------------------------------------------------------
// [7] 批量：1000 条混在一起，只有该到期的被收走
// ---------------------------------------------------------------------------
static void test_bulk_mixed() {
    std::puts("[7] 批量混合");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(1000));

    // 0..499 早就该超时（touch 在 base-2000 之前算的 -> 用负偏移）
    for (int i = 0; i < 500; ++i) s.touch_at(i, at(base, -5000));
    // 500..999 刚刚刷新过
    for (int i = 500; i < 1000; ++i) s.touch_at(i, base);
    CHECK_EQ(s.size(), 1000u);

    std::vector<int> fired;
    CHECK_EQ(s.collect(base, fired), 500u);
    CHECK_EQ(s.size(), 500u);

    // 收集到的都在 [0,500)
    bool all_old = true;
    for (int id : fired) if (id < 0 || id >= 500) all_old = false;
    CHECK(all_old);

    // 再推进到 1000ms，剩下 500 条也跟着到期
    CHECK_EQ(s.collect(at(base, 1000), fired), 500u);
    CHECK_EQ(fired.size(), 1000u);
    CHECK_EQ(s.size(), 0u);
}

// ---------------------------------------------------------------------------
// [8] collect 是「追加」不是「清空」：调用方可以跨两轮攒名单
// ---------------------------------------------------------------------------
static void test_collect_appends() {
    std::puts("[8] collect 追加语义");
    const TimerPoint base = T0();
    IdleScanner s = IdleScanner::make_scan(milliseconds(100));

    s.touch_at(1, base);
    std::vector<int> fired;
    CHECK_EQ(s.collect(at(base, 100), fired), 1u);

    s.touch_at(2, base);
    CHECK_EQ(s.collect(at(base, 200), fired), 1u);
    CHECK_EQ(fired.size(), 2u);   // 上一条还在
}

// ---------------------------------------------------------------------------
// [9] name / timeout 透传
// ---------------------------------------------------------------------------
static void test_meta() {
    std::puts("[9] name / timeout");
    IdleScanner s = IdleScanner::make_scan(milliseconds(500));
    CHECK_EQ(std::string(s.name()), std::string("scan"));
    CHECK_EQ(s.timeout().count(), 500);

    s.set_timeout(milliseconds(1500));
    CHECK_EQ(s.timeout().count(), 1500);
}

int main() {
    std::puts("==== IdleScanner 单元测试 ====");
    test_no_expire_before_deadline();
    test_touch_refreshes();
    test_collect_is_one_shot();
    test_remove();
    test_disabled();
    test_reenable_clears_stale();
    test_bulk_mixed();
    test_collect_appends();
    test_meta();
    std::printf("\n---- pass=%d fail=%d ----\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}