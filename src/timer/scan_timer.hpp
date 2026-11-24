/**
    scan_timer.hpp —— 定时器实现 ①：全表扫描

    结构：std::unordered_map<Id, TimerPoint>
    每次 advance(now) 遍历整张表，deadline <= now 的全部摘掉。

    ==== 为什么第一个实现是它 ====

    ★ 复杂度
      add    O(1)
      cancel O(1)
      advance O(n)      ← 唯一的代价

      500 连接时 n=500，一次全扫 ≈ 500 次 time_point 比较 ≈ 2~3 μs。
      epoll_wait 本来 100ms 醒一次，2μs 占比 0.002%，完全不用优化。

    ★ 什么时候它会成为问题
      n > 10 万时，每个 tick 扫 10 万次比较约 400~600μs，
      占 100ms 的 0.5%，仍然不大。真正的瓶颈不是 CPU，
      而是「每 tick 都要遍历全部连接」这件事本身让缓存不友好。
      到那个量级才换 heap/rbtree，再往上才换 wheel。

    ★ 它的隐藏优势（heap/rbtree 都想抢这个位置）
      1. add 是 O(1)，没有堆调整 —— idle_scanner 每 recv 一次就 add 一次，
         高频刷新场景这是最大头。
      2. 删除安全：unordered_map::erase 后其他迭代器不失效，
         advance 里两阶段摘除写起来最不容易错。
      3. 无惰性删除产生的「幽灵记录」，size() 永远精确。
      本项目的空闲超时是「5 分钟内每条连接都被刷几十次」的模式，
      所以 scan 在 500~5000 连接区间其实是四种里最优的。
      写另外三种是为了让你在真实数据上看到这件事，不是为了替换它。

    ==== 两阶段摘除（对应 timer_queue.hpp 的决策 3）====
      阶段 1：遍历 _map，把到期的 (id, deadline) 收进局部 vector
      阶段 2：先 erase 掉所有到期 id
      阶段 3：才开始逐个 cb(id)
    顺序不能换。如果边遍历边 cb，cb 里 add(同一个 id) 会让
    unordered_map 触发 rehash，正在用的迭代器当场失效 → 崩。
*/

#pragma once

#include "timer_base.hpp"

#include <unordered_map>
#include <utility>
#include <vector>

class ScanTimer final : public TimerQueue {
public:
    ScanTimer() = default;

    void add(Id id, TimerPoint when) override {
        // 同 id 重复 add 就是刷新：operator[] 覆盖值，不会多出一条记录
        _map[id] = when;
    }

    bool cancel(Id id) override {
        return _map.erase(id) > 0;
    }

    std::size_t advance(TimerPoint now, const Callback& cb) override {
        if (_map.empty()) return 0;

        // ---- 阶段 1：持锁外的只读收集 ----
        // 用局部 vector 而不是在遍历中直接删，见文件头「两阶段摘除」
        std::vector<Id> fired;
        for (const auto& kv : _map) {
            if (kv.second <= now) fired.push_back(kv.first);
        }
        if (fired.empty()) return 0;

        // ---- 阶段 2：全部摘除，之后再也不会被扫到 ----
        for (Id id : fired) _map.erase(id);

        // ---- 阶段 3：逐个回调。此时 cb 里 add/cancel 任意 id 都安全 ----
        for (Id id : fired) cb(id);

        return fired.size();
    }

    std::size_t size() const noexcept override { return _map.size(); }

    void clear() noexcept override { _map.clear(); }

    const char* name() const noexcept override { return "scan"; }

private:
    std::unordered_map<Id, TimerPoint> _map;
};