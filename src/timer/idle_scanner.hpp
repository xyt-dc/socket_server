/**
    idle_scanner.hpp —— 把 TimerQueue 粘到「连接」上的那一层

    分层关系（自下而上）：
      timer_types.hpp   时钟口径
      timer_base.hpp    5 个操作的抽象接口
      scan_timer.hpp    其中一种实现（unordered_map 全扫）
      idle_scanner.hpp  ← 本文件：把「id + 超时毫秒」翻译成 add/cancel/advance
      调用方            EpollServer / OLPT_Server

    为什么不让 EpollServer 直接持有 TimerQueue？
      因为 EpollServer 关心的是「连接空闲了多久」，TimerQueue 关心的是
      「id 和绝对时间点」。中间差一层翻译：
        · 「空闲阈值」这个配置只有这一层知道，TimerQueue 不知道也不该知道
        · 「timeout == 0 = 关闭功能」这个开关也只属于这一层
        · 上层只想要「谁超时了」的名单，不想要回调 —— 因为删连接必须
          「持锁收集 → 放锁 → 逐个删」，塞进回调里做就没法分阶段
      把这层剥出来，四个服务器模型可以复用同一份代码。

    ==== 三个必须讲清的设计决策 ====

    ★ 决策 1：timeout == 0 表示「功能关闭」，touch/remove/collect 全部 no-op
      bench_server 的 --idle-timeout=0 就是这个语义。
      如果 0 表示「立即超时」，那么「关闭超时」就无处表达，
      调用方只能自己到处 if，很快会漏掉一处。

    ★ 决策 2：collect() 只作答「谁超时了」，不替调用方删连接
      TimerQueue::advance 的契约是「先全部摘除，再回调」，
      所以 collect 返回的 id 已经从定时器结构里消失了 —— 不会再报第二次。
      但「从 _conns 里 erase + close(fd)」是服务器模型的事：
      每个模型的数据结构不同（Epoll 只有一张表，OLPT 有 N 张），
      而且必须持锁收集、放锁删除。这一层碰锁就是设计错误。

    ★ 决策 3：本类不是线程安全的，只允许「定时器所属线程」调用
      在 EpollServer 里就是唯一的 reactor 线程。
      worker 线程要刷新时间戳时不直接调用 touch()，而是把「刷新」这个动作
      也塞进回写队列，由 reactor 统一执行 —— 和 wbuf / epoll_ctl 同一处理方式。
      加锁也能让它线程安全，但那是把问题往后推：真正的坑是
      「收集到的 fd 在放锁后可能已经被别人删了」，必须靠调用方校验，不是靠锁。
*/

#pragma once

#include "timer_base.hpp"
#include "scan_timer.hpp"

#include <memory>
#include <utility>
#include <vector>

class IdleScanner {
public:
    using Id        = TimerQueue::Id;
    using IdList    = std::vector<Id>;

    /**
     * @param timer   底层实现（scan / heap / rbtree / wheel），本类只当接口用
     * @param timeout 空闲阈值；0 = 关闭该功能
     */
    IdleScanner(std::unique_ptr<TimerQueue> timer, std::chrono::milliseconds timeout)
        : _timer(std::move(timer))
        , _timeout(timeout)
        , _enabled(timeout.count() > 0 && _timer != nullptr)
    {}

    /** @brief 便捷构造：一期只用 scan。后续 --timer= 切换时换成工厂函数 */
    static IdleScanner make_scan(std::chrono::milliseconds timeout) {
        return IdleScanner(std::make_unique<ScanTimer>(), timeout);
    }

    bool enabled() const noexcept { return _enabled; }

    std::chrono::milliseconds timeout() const noexcept { return _timeout; }

    /**
     * @brief 改阈值。传 0 = 关闭；从「关闭」改回正数时清空残留状态
     *
     * 为什么要清空：关闭期间 touch/remove 都是 no-op，定时器里可能
     * 还留着关闭前的旧记录。重新开启后这些记录会以旧 deadline 被扫到，
     * 造成「刚开启就批量踢人」的假象。
     */
    void set_timeout(std::chrono::milliseconds ms) {
        _timeout = ms;
        _enabled = ms.count() > 0 && _timer != nullptr;
        if (!_enabled) reset();
    }

    /**
     * @brief 刷新一个 id 的到期时刻 = 「它刚刚活跃过」
     *
     * 同 id 重复调用是刷新，不是新增（timer_base.hpp 决策 2）。
     * 所以调用方可以放心「每次读写成功都调一次」，不需要先 cancel。
     */
    void touch(Id id) {
        if (!_enabled) return;
        _timer->add(id, timer_deadline(_timeout));
    }

    /** @brief 用外部给的时间点刷新，供单元测试「手工拨表」用（本文件内不产生真实时间依赖） */
    void touch_at(Id id, TimerPoint now) {
        if (!_enabled) return;
        _timer->add(id, now + _timeout);
    }

    /** @brief 连接关闭时调用。漏调 = 定时器里堆幽灵记录 → 内存泄漏 + 误杀复用的 fd */
    bool remove(Id id) {
        if (!_enabled) return false;
        return _timer->cancel(id);
    }

    /**
     * @brief 收集 now 时刻所有已超时的 id，追加到 out 末尾
     * @return 本次新增到 out 的条数
     *
     * ★ 被收集到的 id 已经从定时器里摘除，不会再被报第二次。
     *   调用方必须在同一线程里紧接着把它们从连接表删掉，
     *   否则这个 id 既不在连接表里也不在定时器里 —— 谁都不会再管它。
     */
    std::size_t collect(TimerPoint now, IdList& out) {
        if (!_enabled) return 0;
        const std::size_t before = out.size();
        _timer->advance(now, [&out](Id id) { out.push_back(id); });
        return out.size() - before;
    }

    /** @brief 当前登记的 id 数，用于泄漏检测（连接数上涨而它不涨 = 没在 touch） */
    std::size_t size() const noexcept { return _timer ? _timer->size() : 0; }

    const char* name() const noexcept { return _timer ? _timer->name() : "none"; }

    void reset() noexcept {
        if (_timer) _timer->clear();
    }

private:
    std::unique_ptr<TimerQueue>    _timer;
    std::chrono::milliseconds      _timeout{0};
    bool                           _enabled{false};
};