/**
    timer_types.hpp —— 所有定时器的基类

    为什么单独一个文件？
      四种定时器（scan / heap / rbtree / wheel）内部结构完全不同，
      但「用哪个时钟、时间点用什么类型」必须完全一致。
      否则换个实现就得改调用方，--timer= 切换也就成了假切换。

    ★ 为什么必须是 steady_clock，不能是 system_clock
      system_clock 是「墙钟」，会被 NTP / 手动改时间影响：
        · 往前跳 1 小时 → 所有连接瞬间全超时，集体误杀（线上事故）
        · 往后跳 1 小时 → 该断的连接一小时内不断，慢连接越堆越多
      这两种情况在本地测试永远测不出来，因为你的机器时钟不会跳。
      steady_clock 只保证「单调递增」（不管日期对不对），
      正好是定时器唯一需要的东西。

    ⚠️ 不要图省事把 now() 存成 int64 毫秒
      存 time_point 才自带类型和精度，不会被「有人以为是秒」这种错误
      静默吞掉。真要打日志再 .time_since_epoch().count() 转。
*/

#pragma once

#include <chrono>
#include <cstdint>

// 统一加 Timer 前缀，避免和项目里其他 Clock / TimePoint 撞名。
// 本项目不包 namespace，全靠类名前缀区分家族（看 lib/ 和 src/mthread/）。
using TimerClock    = std::chrono::steady_clock;
using TimerPoint    = TimerClock::time_point;
using TimerDuration = TimerClock::duration;

/**
 * @brief 把「从现在起多少毫秒」换成绝对时间点
 *
 * 定时器内部只认绝对时间点，不认剩余时长 ——
 * 剩余时长每读一次就已经过期了，存它就是失真。
 */
inline TimerPoint timer_deadline(std::chrono::milliseconds x_ms) noexcept {
    return TimerClock::now() + x_ms;
}

/**
 * @brief 两个时间点之间的毫秒数（测试断言和日志用）
 */
inline int64_t timer_ms_between(TimerPoint from, TimerPoint to) noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(to - from).count();
}