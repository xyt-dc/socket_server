/**
    timer_queue.hpp —— 四种定时器的共同接口

    四种实现（scan / heap / rbtree / wheel）内部结构完全不同：
      scan   : unordered_map<id, deadline>，每次 tick 全表扫
      heap   : 小根堆 + 惰性删除
      rbtree : multimap<deadline, id>
      wheel  : 固定槽数 + 圈数
    但对外只暴露下面这 5 个操作。调用方（idle_scanner / EpollServer）
    完全不知道背后是哪种 —— 这就是 --timer=scan|heap|rbtree|wheel
    能真切换的前提。

    ==== 三个必须讲清的设计决策 ====

    ★ 决策 1：只存绝对时间点，不存「剩余多少毫秒」
      剩余时长每次读出来就已经不是原来的值了。存 time_point，
      重算差值永远是「现在到那一刻还有多久」。

    ★ 决策 2：add() 同 id 两次 = 刷新，不是新增
      idle_scanner 每次 recv 成功都会 add 一次同 id。
      四种实现都必须保证「同 id 只有一份有效记录」，
      否则连接会被越刷越多份，超时判断全乱。

    ★ 决策 3：advance() 先把到期项全部摘下来，再回调
      如果边遍历边回调，cb 内部一旦 cancel/add 同一个 id，
      scan 会迭代器失效，heap 会堆结构损坏。
      所以契约写成：
        advance(now, cb)
          1) 把 deadline <= now 的记录从内部结构【全部摘除】
          2) 逐个 cb(id)
      这样 cb 里可以安全地 add/cancel 任意 id（包括刚 fired 的那个）。
      返回值 = 本次触发了几条，测试和日志都用它。

    ⚠️ 回调是「在扫描线程里同步执行」的。
      回调里不能做阻塞操作、不能 sleep —— 那会让整个 reactor 停摆。
      需要慢动作请自己丢到别的队列去。
*/

#pragma once

#include "timer_types.hpp"

#include <cstddef>
#include <functional>
#include <string_view>

class TimerQueue {
public:
    using Id       = int;
    using Callback = std::function<void(Id)>;

    virtual ~TimerQueue() = default;

    /**
     * @brief 注册 / 刷新一个 id 的到期时刻
     * @param id    业务侧的唯一编号（本项目直接用 client fd）
     * @param when  绝对到期时刻，用 timer_deadline(ms) 算
     *
     * 同 id 重复调用 = 覆盖成新的 when（刷新），不会产生重复记录。
     */
    virtual void add(Id id, TimerPoint when) = 0;

    /**
     * @brief 取消一个 id
     * @return true = 原来存在；false = 本来就没有
     *
     * 取消后不会再被 cb 回调。已经 fired 的（上一轮 advance 摘掉的）取消返回 false。
     */
    virtual bool cancel(Id id) = 0;

    /**
     * @brief 推进到 now，摘除所有到期记录并逐个回调
     * @return 本次触发的条数
     */
    virtual std::size_t advance(TimerPoint now, const Callback& cb) = 0;

    /** @brief 当前有效记录数（用于泄漏检测，四种实现都必须能 O(1) 或 O(n) 给出精确值） */
    virtual std::size_t size() const noexcept = 0;

    /**
     * @brief 丢弃全部记录，不回调
     *
     * 唯一用途：IdleScanner 从「超时关闭(timeout=0)」切回「开启」时清掉旧记录。
     * 关闭期间 add/cancel 都是 no-op，表里可能还留着关闭前的 deadline，
     * 重新开启后会立刻被扫到 → 「刚开启就批量踢人」。
     * 注意它不等于「让全部到期」，不产生任何回调。
     */
    virtual void clear() noexcept = 0;

    /** @brief 实现名，供日志 / --timer= 回显 */
    virtual const char* name() const noexcept = 0;
};

/** @brief 推荐 tick 间隔：epoll_wait 本来就 100ms 醒一次，顺手扫一遍，零额外代价 */
inline constexpr std::chrono::milliseconds kTimerTick{100};