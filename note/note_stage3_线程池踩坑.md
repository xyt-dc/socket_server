# 阶段3：线程池 —— 踩坑笔记

对应文件：`lib/blocking_queue.hpp`、`lib/Iothread_pool.hpp`、`lib/thread_pool.hpp`（Classical 版）

这篇文档只记录**我本人真实犯过**的错误，每条都附实测证据和修法。

***

## 目录

- [一、编译期错误](#一编译期错误)
  - 坑 1：`std::function<void>` 少了 `()`
  - 坑 2：`std:` 少一个冒号
  - 坑 3：`_task` / `empalce` 拼写
  - 坑 4：`std::forward(f)` 少了模板参数
  - 坑 5：`...args` 和 `args...` 不是一回事
  - 坑 6：`if` 不带大括号，缩进骗人
  - 坑 7：`std::atomic_bool` 没 include `<atomic>`
- [二、运行期崩溃](#二运行期崩溃)
  - 坑 8：`catch(const std::exception&)` 抓不住 `throw 42`
- [三、并发正确性](#三并发正确性)
  - 坑 9：`pop` 返回 false 后什么都不做 → 100% CPU 空转
  - 坑 10：`shutdown` 不幂等 → 并发调用直接崩
  - 坑 11：`submit` 用了阻塞版 `push` → accept 卡死 2 秒
  - 坑 12：`bool submit` 忘了 `return` → 未定义行为
- [四、设计层面](#四设计层面)
  - 坑 13：`std::bind` 会吞掉完美转发
  - 坑 14：成员声明顺序
  - 坑 15：`[&]` vs `[this]`
  - 坑 16：`std::atomic<bool>` 做"只做一次"标志是错的
- [五、测试方法论](#五测试方法论)
  - 坑 17：断言写成了"跑 200 个必须执行 200 个"
  - 坑 18：`assert` 在 `-DNDEBUG` 下会凭空消失
- [六、速查表](#六速查表)

***

# 一、编译期错误

## 坑 1：`std::function<void>` 少了 `()`

**症状**：报了 10 个错，看得头皮发麻。

```
error: no match for 'operator=' (operand types are 'std::function<void()>'
       and 'std::remove_reference<std::function<void>&>::type')
error: invalid use of incomplete type 'std::function<void>'
error: cannot increment a pointer to incomplete type 'std::function<void>'
...
```

**错误写法**：

```cpp
std::queue<std::function<void>> _tasks;      // ← 错
```

**正确写法**：

```cpp
std::queue<std::function<void()>> _tasks;    // ← 对
```

**为什么**：`std::function` 的模板参数是**函数签名**，不是返回类型。`void` 不是签名，`void()` 才是（"无参数、无返回值"）。写错了它就是个**不完整类型**，于是所有用到它的地方（`deque` 内部、赋值、析构）全线报错 —— **10 个错误里 7 个是这一个引起的**。

**找错的心法**：编译错误数量多的时候，**只看第一个**。后面全是连锁反应。修掉第一个，重新编译，看新的第一个。

***

## 坑 2：`std:` 少一个冒号

**症状**：

```
error: found ':' in nested-name-specifier, expected '::'
```

**错误写法**：

```cpp
using return_type = typename std:result_of<F(Args...)>::type;
//                                  ^ 只有一个冒号
```

**正确写法**（顺带把 deprecated 的 `result_of` 换掉）：

```cpp
using return_type = std::invoke_result_t<F, Args...>;
```

**为什么换**：`std::result_of` **C++17 已废弃、C++20 已删除**。`std::invoke_result_t` 是它的替代品，而且不用写 `typename ... ::type` 那一长串。

**顺带一个坑**：`std::result_of<F(Args...)>` 里那个 `F(Args...)` 是**函数类型写法**，不是调用。这是 `result_of` 的特殊语法，用多了容易误以为在调用函数。`invoke_result_t<F, Args...>` 就直白多了。

***

## 坑 3：`_task` / `empalce` 拼写

**症状**：

```
error: '_task' was not declared in this scope; did you mean '_tasks'?
```

**错误写法**：

```cpp
_task.empalce([task](){ (*task)(); });      // 两处都错
```

**正确写法**：

```cpp
_tasks.emplace([task](){ (*task)(); });
```

`empalce` 是 `emplace` 的字母顺序打反了（m-p-l → p-l-m）。这种错编译器给的提示很明确，**但前提是你真的去读错误信息**。

***

## 坑 4：`std::forward(f)` 少了模板参数

**症状**：

```
error: no matching function for call to 'forward(main()::<lambda()>&)'
note:   couldn't deduce template parameter '_Tp'
```

**错误写法**：

```cpp
return _tasks.push(Task(std::forward(f)));       // ← 错
```

**正确写法**：

```cpp
return _tasks.push(Task(std::forward<F>(f)));    // ← 对
```

**为什么**：`std::forward<T>(x)` 的 `T` **不能推导**，必须手写。

对比记忆：

| 函数 | 能推导吗 | 原因 |
|---|---|---|
| `std::move(x)` | ✅ 能 | 只有一个模板参数，就是 `x` 的类型 |
| `std::forward<T>(x)` | ❌ 不能 | 有两个"参数"（`T` 和 `x`），编译器不知道你想要哪个 |

`std::forward` 的本质是"**我要把它按 `T` 的原始值类别人发出去**"，这个 `T` 是你在模板参数列表里已经声明的那个，编译器没法从 `x` 反推。

***

## 坑 5：`...args` 和 `args...` 不是一回事

**症状**：

```cpp
// C++17 下：
warning: pack init-capture only available with '-std=c++20' or '-std=gnu++20'

// 在捕获列表里写成 args...：
error: expected ',' before '...' token
```

**两种 `...` 的含义完全不同**：

| 写法 | `...` 位置 | 叫什么 | 含义 |
|---|---|---|---|
| `Args... args` | 名字**前** | **打包** | "这是一个参数包" |
| `args...` | 名字**后** | **展开** | "把里头的东西一个个摊开" |

**口诀：`...` 永远贴在"要被展开的那个东西"后面。**

实际规则：

```cpp
template<class... Args>                    // ✅ 声明包
void f(Args... args)                       // ✅ 声明包（... 跟类型）
{
    g(args...);                            // ✅ 展开（... 跟变量）
    g(std::forward<Args>(args)...);        // ✅ 展开（... 跟整个表达式）
}
```

**lambda 捕获列表里是特例**：

```cpp
[...args = std::forward<Args>(args)]       // ✅ 但这是 C++20 才有的语法
[args = std::forward<Args>(args)...]       // ❌ 永远非法，C++20 也不行
```

**你的项目锁 C++17，所以两者都不能用。** 唯一替代方案（实测通过）：

```cpp
auto task = std::make_shared<std::packaged_task<return_type()>>(
    [f = std::forward<F>(f),
     tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        return std::apply(std::move(f), std::move(tup));
    });
```

注意这里 `std::make_tuple(std::forward<Args>(args)...)` 的 `...` 在**括号里面**，它是普通的参数展开，跟捕获列表无关，C++17 完全合法。需要 `#include <tuple>`。

**实测结果（C++17，`-Wall -Wextra` 零警告）**：

```
普通参数: n=42
unique_ptr: 42          ← bind 版本在这里会编译失败
std::ref: 5 (原变量=5)
```

***

## 坑 6：`if` 不带大括号，缩进骗人

**错误写法**：

```cpp
if(_stop)
    throw std::runtime_error("enqueue on stopped ThreadPool");
    _tasks.emplace([task](){      // ← 缩进看起来在 if 里，实际【不在】
        (*task)();
    });
```

`if` 只管辖**紧跟的那一条语句**（`throw`）。`_tasks.emplace` 是独立的下一条语句，**永远执行**。

现在能跑纯属侥幸：`throw` 直接退出函数，所以 `emplace` 只在 `!_stop` 时被走到。**但哪天你把 `throw` 改成 `return;`**，`_tasks.emplace` 就会在池子已停止时继续塞任务 —— 那个任务**永远没人执行**，而调用方拿着一个永远 pending 的 `future` 死等。

**正确写法（单行也必须加括号）**：

```cpp
if(_stop){
    throw std::runtime_error("enqueue on stopped ThreadPool");
}
_tasks.emplace([task](){ (*task)(); });
```

**结论**：`if` / `for` / `while` 后面**无条件加大括号**。这是 Google、LLVM 等所有主流 C++ 规范的共同要求，原因就是这一个。

***

## 坑 7：`std::atomic_bool` 没 include `<atomic>`

**症状**（clangd `--check`）：

```
E [typename_nested_not_found] Line 15: no type named 'atomic_bool' in namespace 'std'
```

**错误写法**：

```cpp
#include "blocking_queue.hpp"
#include "jthread.hpp"
#include <functional>
#include <vector>

class ThreadPool {
private:
    std::atomic_bool done;      // ← 没 include <atomic>
};
```

**为什么**：`<thread>`、`<mutex>`、`<functional>` 在某些 libstdc++ 版本下会**间接**带进 `<atomic>`，所以有时能编译。**这是运气，不是保证** —— 换个编译器版本、换个标准库就炸，而且报错位置离真正原因很远（它不会说"你少 include 了"，只会说"没有 atomic_bool 这个类型"）。

**结论**：**自己用到的标准库类型，自己 include 对应的头文件**。不要依赖间接包含。

| 用到 | 必须 include |
|---|---|
| `std::atomic_bool` / `std::atomic<int>` | `<atomic>` |
| `std::forward` / `std::move` / `std::pair` | `<utility>` |
| `std::make_tuple` / `std::apply` / `std::get` | `<tuple>` |
| `std::call_once` / `std::once_flag` / `std::mutex` | `<mutex>` |
| `size_t`（不带 `std::` 前缀） | `<cstddef>` |
| `std::cerr` / `std::cout` | `<iostream>` |

***

# 二、运行期崩溃

## 坑 8：`catch(const std::exception&)` 抓不住 `throw 42`

**这是我犯的最危险的一个错误。** 写完 `try` 就以为安全了，实际还是会被 `terminate()`。

**错误写法**：

```cpp
try{
    out();
}catch(const std::exception& e){
    std::cerr << e.what() << '\n';
}
```

**实测证据**（提交一个 `throw 42;` 的任务）：

```
=== 用例3: throw 42 (不是 std::exception) ===
terminate called after throwing an instance of 'int'
Aborted
exit=134 (134=SIGABRT)
```

**整个进程被干掉。** 因为 `42` 是 `int`，不派生自 `std::exception`，`catch(const std::exception&)` 抓不住 → 异常穿出线程入口函数 → C++ 规定 **`std::terminate()`**。

**正确写法（两级，顺序不能反）**：

```cpp
try{
    out();
}catch(const std::exception& e){
    std::cerr << "task threw: " << e.what() << '\n';
}catch(...){                          // ← 必须放最后
    std::cerr << "task threw unknown\n";
}
```

或者最简版：

```cpp
try{
    out();
}catch(...){
    // 什么都抓，进程活着
}
```

**为什么必须两级**：只写 `catch(...)` 能保命但拿不到信息；只写 `catch(const std::exception&)` 有信息但会崩。两个都写，既能拿信息又保命。

**`catch(...)` 必须在最后** —— 它是"兜底"，放前面的话后面所有 `catch` 都进不去（编译器可能直接警告 unreachable）。

**必须包 try/catch 的三个地方**：

| 位置 | 不包的后果 |
|---|---|
| 线程入口函数（`worker_loop` 的 `task()` 调用） | 整个进程 `terminate` |
| 信号处理函数内部 | 同上，而且更糟（信号上下文里不能抛） |
| `std::thread` 的 lambda 体 | 同上 |

**实测修好后**：

```
=== 用例3: throw 42 ===
task threw unknown
后续任务执行数 = 1      ← 后续任务照跑，进程活着
exit=0
```

***

# 三、并发正确性

## 坑 9：`pop` 返回 false 后什么都不做 → 100% CPU 空转

**这是最隐蔽的一个。** 程序不崩、不报错，就是**卡死 + 烧 CPU**。

**错误写法**：

```cpp
void worker_loop() {
    while (true)
    {
        Task out;
        if(_tasks.pop(out)){
            out();
        }else
        {
            /* code */          // ← 空的！pop 返回 false 后什么都不做
        }
    }
}
```

`pop` 返回 `false` 只有**一种**可能：**队列已关闭且为空**（worker 该下班了）。但这里什么都没做，于是 `while(true)` 立刻再来一轮 `pop`，又立刻返回 `false`，又循环……

**实测证据**（2 个 worker，卡住 2 秒后）：

```
=== 卡住 2 秒后的 CPU 占用 ===
   PID USER   PR NI  VIRT  RES  SHR S  %CPU  %MEM     TIME+ COMMAND
1785600 root  20   0 22760 3644 3424 S 172.7   0.0   0:04.41 io_cpu

=== 各线程状态 (R=运行/S=睡眠) ===
   PID     TID %CPU STAT COMMAND
1785600 1785600  0.0 Sl   io_cpu
1785600 1785602  102 Rl   io_cpu      ← worker 1：102%
1785600 1785603  102 Rl   io_cpu      ← worker 2：102%
```

**172.7% CPU**，两个 worker 都是 `R`（Running）。它们**不是在执行任务**，是在拿 `pop` 刷空转。

更糟：`pop` 内部要抢锁，所以每一轮空转都要 `lock/unlock` 一次 mutex，两个线程互相抢 —— 把锁总线都打满了，还会**饿死真正想干活的其他线程**（这就是为什么之前的测试里"提交 3 个任务只执行了 1 个"）。

**正确写法（卫语句，一行解决）**：

```cpp
void worker_loop() {
    for (;;) {
        Task out;
        if (!_tasks.pop(out)) {
            return;                 // 队列关了且空了 = 我该下班了
        }
        try { out(); } catch (...) { }
    }
}
```

**顺带**：这种"`if(成功){干活} else {什么都不做}`再回到循环开头"的模式，几乎总是错的。要么 `return`/`break`，要么 `continue` 前必须做点什么（比如 `sleep`）。**空 `else` + 无限循环 = 100% CPU**。

**怎么发现这类问题**：`top -H -p <pid>` 看每个线程的 CPU。**空闲时线程应该是 `S`（sleeping），不是 `R`（running）。**

***

## 坑 10：`shutdown` 不幂等 → 并发调用直接崩

**错误写法**：

```cpp
void shutdown() {
    _tasks.close();
    for(auto& worker : _workers){
        if (worker.joinable()) worker.join();
    }
}
```

单线程调用它**完全正确**（这点没错）。问题在**两个线程同时调**的时候。

**实测证据（TSan，200 轮并发 shutdown）**：

```
ThreadSanitizer: CHECK failed: sanitizer_thread_registry.cpp:348
    #5 IoThreadPool::shutdown() fixed_pool.hpp:56
exit=66
```

**直接崩。** 原因是经典 TOCTOU（Time-of-check to time-of-use）：

```
线程A: close() → 遍历 _workers → worker[0].joinable() == true
线程B:            close() → 遍历 _workers → worker[0].joinable() == true
                                             ↑ 两个线程都看到 true
线程A: worker[0].join()      ← 开始 join
线程B: worker[0].join()      ← 同时 join 同一个线程 → 未定义行为
```

`joinable()` 检查和 `join()` 之间**有间隙**，另一个线程插进来了。

而且 `_workers` 这个 vector 本身被两个线程**无锁遍历**，就算不崩也是数据竞态。

**三种修法对比**：

| 方案 | 代码 | 优点 | 缺点 |
|---|---|---|---|
| **A. `std::call_once`** ⭐ | `std::call_once(_once, [this]{...});` | 语义最准确（"只做一次"）、线程安全由标准库保证、异常安全 | 多一个成员 |
| B. `std::mutex` | 整个函数用 `lock_guard` 包住 | 简单直观 | 持锁期间可能阻塞几秒；是"串行做两次"不是"只做一次" |
| C. `std::atomic<bool>` | `if(_stopped.exchange(true)) return;` | 无锁、快 | **有竞态**（见坑 16） |

**正确写法（A 方案，实测 TSan 干净）**：

```cpp
    void shutdown() {
        std::call_once(_once, [this] {
            _tasks.close();                                  // 1. 叫醒所有阻塞者
            for (auto& w : _workers) {
                if (w.joinable()) w.join();                   // 2. 等 worker 排空退出
            }
            _workers.clear();                                 // 3. 清掉线程对象
        });
    }

private:
    BlockingQueue<Task>      _tasks;
    std::vector<std::thread> _workers;
    std::once_flag           _once;      // ← 新增
```

**实测结果**：

```
=== 200 轮并发 shutdown（TSan）===
200 轮并发 shutdown 完成
exit=0 (0=无竞态)
```

**`std::call_once` 为什么天然幂等**：

```
第一次调用: once_flag 未标记 → 执行 lambda → 标记为已执行
第二次调用: once_flag 已标记 → 立刻返回，不执行 lambda
```

而且如果 lambda 里**抛了异常**，`once_flag` 会**重置**，下次还能重试。这点 `std::atomic<bool>` 做不到。

**`_workers.clear()` 为什么要有**：join 完的 `std::thread` 对象还在 vector 里，虽然它们析构无害，但 `clear()` 让 `worker_count()` 返回 0，语义更准确，也避免"看起来还有 4 个线程"的误导。

***

## 坑 11：`submit` 用了阻塞版 `push` → accept 卡死 2 秒

**这是本次最严重的设计错误，而且它"能跑通所有功能测试"，极难发现。**

**错误写法**：

```cpp
template <typename F>
bool submit(F&& f) {
    return _tasks.push(Task(std::forward<F>(f)));   // ← push 是【阻塞】版
}
```

而 `BlockingQueue::push` 是这样的：

```cpp
bool push(T item){
    std::unique_lock<std::mutex> lk (_mtx);
    _not_full.wait(lk, [&](){return _queue.size()<_cap||_closed;});
    //       ^^^^ 满了就在这里睡，一直睡到有空间
    ...
}
```

**实测证据**（1 个 worker + 队列 cap=2，worker 被一个 `sleep(2s)` 的任务占住）：

```
=== 测试 submit 在队列满时是否阻塞 ===
  submit #0 -> true,  耗时 0 ms
  submit #1 -> true,  耗时 0 ms
  submit #2 -> true,  耗时 2000 ms      ← ★ 卡了整整 2 秒
  submit #3 -> true,  耗时 0 ms
4 次 submit 总耗时 2000 ms
```

**这直接摧毁了整个设计的核心价值。**

你设计 `bool submit` 就是为了"满了立刻告诉我，我 `close(fd)` 拒绝新连接"。但 `push` 会睡到有空间为止 —— 放在 accept 循环里，就是：

```
accept 循环（单线程、关键路径）
    ├─ accept 拿到 fd
    ├─ submit(...) → 队列满 → 【睡 2 秒】
    │                              ↑ 这 2 秒里：
    │                                - accept 停摆，新连接堵在 listen backlog
    │                                - 老连接占着 fd 不释放，fd 数暴涨
    │                                - 客户端全部超时
    └─ 醒来，继续
```

**本来是为了防过载，结果造成了更严重的过载。**

**正确写法 —— `BlockingQueue` 加一个 `try_push`**：

```cpp
    // 非阻塞版 push：满了立刻返回 false，绝不睡眠
    bool try_push(T item){
        std::unique_lock<std::mutex> lk(_mtx);
        if (_queue.size() >= _cap) return false;   // 满 → 立刻走人
        if (_closed) return false;
        _queue.push_back(std::move(item));
        lk.unlock();
        _not_empty.notify_one();
        return true;
    }
```

`submit` 改成：

```cpp
    template <typename F>
    bool submit(F&& f) {
        return _tasks.try_push(Task(std::forward<F>(f)));
    }
```

**实测结果**：

```
  submit #0 -> true,  耗时 0 ms
  submit #1 -> false, 耗时 0 ms      ← 满了，立刻拒绝
  submit #2 -> false, 耗时 0 ms
  submit #3 -> true,  耗时 0 ms
4 次 submit 总耗时 0 ms              ← ★ 真正非阻塞
```

**三种"队列满"策略，按业务选**：

| 策略 | 方法 | 适用场景 |
|---|---|---|
| **满了拒绝** | `try_push` | **服务端**：宁可拒绝新连接，也不能拖慢 accept 循环 |
| **满了丢最老** | `force_push`（覆盖 `front()`） | 监控指标、日志：新的比旧的更有价值 |
| **满了阻塞** | `push` | 离线批处理：必须全部做完，慢点无所谓 |

**关键认知**：`BlockingQueue` 三个方法**各有各的用途，不能乱用**。

```cpp
push(x)       // 阻塞版，满了睡。适合"任务必须完成"的场景
try_push(x)   // 非阻塞版，满了返回 false。适合"过载要能感知"的场景
pop(out)      // 阻塞版，空了睡。worker 线程用它
```

**`try_push` 与你原有 10 个测试的关系**：`test_02_blocking_queue.sh` 测的是 `push`/`pop`/`close`，`try_push` 是**纯新增**，不影响已有行为。我实测重跑过，27 个断言全过。

***

## 坑 12：`bool submit` 忘了 `return` → 未定义行为

**症状**：

```
warning: no return statement in function returning non-void [-Wreturn-type]
```

**错误写法**：

```cpp
template <typename F>
bool submit(F&& f) {
    if(!_tasks.push(...)){
        // 什么都没写
    }
    // 函数末尾没有 return
}
```

**这不是"返回随机值"这么简单 —— 是未定义行为。** GCC 在某些优化级别下会让调用方拿到**上一个函数的返回值**（寄存器残留），行为完全不可预测。

**正确写法**：

```cpp
template <typename F>
bool submit(F&& f) {
    return _tasks.try_push(Task(std::forward<F>(f)));
}
```

**心法**：只要函数签名里有返回类型（非 `void`），**每一个出口都必须 return**。写完先扫一眼有没有 `return` 可能在某个分支漏掉。

`-Wall -Wextra` **能抓到这个**，所以这两个 flag 一定要开。

***

# 四、设计层面

## 坑 13：`std::bind` 会吞掉完美转发

**错误写法（Classical 版里）**：

```cpp
auto task = std::make_shared<std::packaged_task<return_type()>>(
    std::bind(std::forward<F>(f), std::forward<Args>(args)...));
```

看起来完美转发写得一丝不苟，**但实际全废了**。

**为什么**：`std::bind` 内部会把参数**按值拷贝**（`decay` 之后）存进一个 `tuple`。所以：

1. 你的 `std::forward<Args>` 白写了 —— 一进 `bind` 就退化成拷贝
2. **`std::unique_ptr` 这类只可移动的参数完全传不进去**（会尝试拷贝而编译失败）

**C++17 的正确写法（tuple + apply）**：

```cpp
#include <tuple>

auto task = std::make_shared<std::packaged_task<return_type()>>(
    [f = std::forward<F>(f),
     tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        return std::apply(std::move(f), std::move(tup));
    });
```

**实测三种参数都通过**：

```
普通参数: n=42
unique_ptr: 42          ← bind 版本在这里会编译失败
std::ref: 5 (原变量=5)
```

**顺带**：你的**服务端版完全不需要这一套**。因为 `submit` 不要返回值，用法是：

```cpp
pool.submit([this, fd]{ handle_connection(fd); });   // 参数在 lambda 捕获里，不用转发
```

**`packaged_task` + `future` + `std::bind` 三件套的价值只在"需要返回值"时**，服务端收连接这点小事用不上。**少一个 `future` 就少一次堆分配 + 一组原子操作**。

***

## 坑 14：成员声明顺序

**你原来的写法**：

```cpp
private:
    std::vector<std::thread> _workers;   // 先声明
    std::queue<...> _tasks;              // 后声明
    std::mutex _mtx;
    std::condition_variable _condition;
    bool _stop;
```

**C++ 规则**：

1. **构造顺序 = 声明顺序**（从上到下）
2. **析构顺序 = 声明顺序的倒序**（从下到上）

所以析构顺序是：`_stop` → `_condition` → `_mtx` → `_tasks` → `_workers`。

**这里有个隐患**：如果哪天你**忘了在析构函数里 join**，那么 `_tasks` 会先于 `_workers` 被销毁，而 worker 还在跑、还在 `_tasks.pop()` → **use-after-free**。

**正确写法 —— 把 `_tasks` 声明在 `_workers` 之前**：

```cpp
private:
    BlockingQueue<Task>      _tasks;     // ← 先声明（后析构）
    std::vector<std::thread> _workers;   // ← 后声明（先析构）
    std::once_flag           _once;
```

这样倒序析构时，**`_workers` 先被销毁**（`std::thread` 析构会 join 或 terminate），然后再销毁 `_tasks`。**即使忘了写 join，靠声明顺序也不会炸。**

**为什么**：`std::thread` 析构时如果还 `joinable()` 会 `std::terminate()` —— 这个"暴力"行为反而保证了**线程先死，资源后销毁**。这是零成本的安全网。

**顺带**：`_workers.clear()` 之后 `worker_count()` 返回 0，这个 `0` 是准确的；不 `clear()` 的话 vector 里还躺着几个已 join 的 thread 对象，`worker_count()` 会撒谎。

***

## 坑 15：`[&]` vs `[this]`

**错误写法**：

```cpp
_workers.emplace_back([&](){
    worker_loop();
});
```

**功能上没错** —— 因为 lambda 里只用到成员（走 `this->`），`[&]` 和 `[this]` 在这里等价。

**但有两个问题**：

1. **误导读者**：`[&]` 让人以为你捕获了局部变量，实际没有
2. **引雷**：将来你在 lambda 前面加个局部变量 `int x`，`[&]` 会**悄悄把 `x` 的引用也捕获**，而 worker 活得比构造函数久 → **悬垂引用 UB**

**正确写法**：

```cpp
_workers.emplace_back([this](){
    worker_loop();
});
```

**`mutable` 也不需要**：`mutable` 是给"要修改**按值**捕获的变量"用的。你这里什么都没按值捕获，加了它只会让 lambda 的 `operator()` 变成非 const，属于无意义的噪音。

**心法**：**捕获列表永远写最小集合**。需要 `this` 就写 `[this]`，需要 `fd` 就写 `[this, fd]`。永远别用 `[&]` / `[=]` 图省事。

***

## 坑 16：`std::atomic<bool>` 做"只做一次"标志是错的

看起来最直观的方案，**但是错的**：

```cpp
// ❌ 错误方案
void shutdown() {
    if (_stopped.exchange(true)) return;     // 已经有人在关了，我撤
    _tasks.close();
    for (auto& w : _workers) if (w.joinable()) w.join();
}
```

**问题**：

```
线程A: exchange(true) → false → 继续往下走，开始 join
线程B: exchange(true) → true  → 【立刻返回】
                                ↑ 调用方以为"已经关闭完了"
                                  实际 A 还在 join，可能还要几秒
```

**这是比崩更糟的 bug —— 静默的语义错误。** B 的调用方会以为"池子已完全关闭，资源已释放"，然后去 `delete` 这个池子 → 而 A 还在里面跑 → **use-after-free**。

**`std::atomic<bool>` 只能保证"标志被设置一次"，不能保证"动作被完成一次顺带其他人等到它完成"。**

| 需求 | 正确工具 |
|---|---|
| "只设置一次标志" | `std::atomic<bool>::exchange` |
| **"只执行一次动作，其他人等到它做完"** | **`std::call_once`** ⭐ |
| "只执行一次但不等"（fire-and-forget） | `atomic` + 各处都容忍"还没做完" |

**`std::call_once` 的语义恰好是第二个**：其他线程**会阻塞等到第一次执行完成**，而不是立刻返回。

**额外优势**：如果 lambda 抛异常，`once_flag` 会**重置**，下次还能重试。`atomic<bool>` 做不到（置了 true 就永远 true，一次失败永久失败）。

***

# 五、测试方法论

## 坑 17：断言写成了"跑 200 个必须执行 200 个"

**我给你的测试用例本身是错的。**

```cpp
// "提交 200 个任务，断言执行数 == 200"   ← 这个断言在【有界队列】下是错的
for(int i=0;i<200;++i) p.submit([&n]{ n.fetch_add(1); });
p.shutdown();
assert(n == 200);                    // ← 会失败
```

**实测**：

```
提交 200 个任务
析构完成，执行数 = 146 / 200
exit=1                               ← 断言失败
```

**不是程序有 bug，是断言写错了。** 队列只有 8 个位置，`try_push` 满了会返回 `false`，**被拒绝的 192 个任务本来就不该执行**。

**正确的不变量是**：

> **被接受的任务（）必须全部执行完。**

```cpp
std::atomic<int> accepted{0}, ran{0};
{
    IoThreadPool p(2, 8);
    for(int i=0;i<200;++i){
        if(p.submit([&ran]{ ran.fetch_add(1); }))   // ← 只数被接受的
            accepted.fetch_add(1);
    }
}
assert(ran.load() == accepted.load());              // ← 正确的不变量
```

**实测**：

```
提交 200 个，被接受 8 个
实际执行 = 8 / 接受 8  ← 必须相等
exit=0
```

**这条经验很重要**：**写测试前先把"什么算正确"想清楚**。测试失败时，第一反应不该是"代码有 bug"，而是"**要么代码错了，要么断言错了**"。

**"优雅退出"真正要验的是什么**：

| 要验的 | 怎么测 |
|---|---|
| 已入队的任务不丢 | 用**无界**队列，或 `accepted == ran` |
| worker 会排空后才退出 | 提交任务后**立刻** shutdown，看任务是否全部跑完 |
| 不会死锁 | 给测试加 `timeout` |
| 不会空转 | 观察 CPU 占用 |

**"提交 200 个立刻 shutdown"依然是极有价值的用例** —— 它验的是"**worker 会排空队列，而不是立刻退出丢掉任务**"。只是断言要用 `accepted == ran`。

***

## 坑 18：`assert` 在 `-DNDEBUG` 下会凭空消失

**错误写法**：

```cpp
assert(pool.queue_size() == 5);      // -DNDEBUG（Release）下这行【完全消失】
```

**后果**：测试在 Release 下"永远通过"，因为它什么都没检查。更糟的是 `assert(x)` 里的**副作用也会消失**：

```cpp
assert(pool.submit(task));          // ← Release 下 submit 根本不会被调用！
```

**正确写法 —— 自己写 `check()`**：

```cpp
static int g_pass = 0, g_fail = 0;

void check(bool ok, const std::string& name) {
    if (ok) { ++g_pass; }
    else    { ++g_fail; printf("  [FAIL] %s\n", name.c_str()); }
}

int main() {
    check(pool.queue_size() == 5, "queue_size");
    printf("汇总：通过 %d，失败 %d\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
```

**三个好处**：

1. 不受 `NDEBUG` 影响，永远执行
2. **不会 abort** —— 一个失败后继续跑剩下的，一次就能看到所有问题
3. 能统计数量，一眼看出严重程度

**顺带**：`return g_fail == 0 ? 0 : 1;` 让 shell 脚本能用 `$?` 判断成败 —— 这是 [test_02_blocking_queue.sh](../test/test_02_blocking_queue.sh) 和 cpp 测试之间的唯一接口。

***

# 六、速查表

## 编写线程池时的检查清单

写完后**逐条打勾**：

### 编译期

- [ ] `std::function<void()>` 后面有 `()`
- [ ] 所有 `std::forward<T>` 都写了 `<T>`
- [ ] 所有 `if` / `for` / `while` 都有 `{}`
- [ ] 模板里没有用 C++20 的包捕获（`[...args = ...]`）
- [ ] 自己用到的标准库类型都自己 `#include` 了
- [ ] 函数每个出口都有 `return`

### 运行期安全

- [ ] 线程入口函数的任务调用包了 `try/catch`，且有 `catch(...)` 兜底
- [ ] `worker_loop` 里 `pop` 返回 `false` 时**立刻 `return`**，不是空 `else`
- [ ] `shutdown()` 用 `std::call_once` 或互斥量保证幂等
- [ ] `shutdown()` 顺序是 **close → join → clear**，不能反
- [ ] 错误路径上也有 `return`（不是所有分支都返回值 → UB）

### 设计

- [ ] `_tasks` 声明在 `_workers` **之前**
- [ ] lambda 捕获列表写的是 `[this]` / `[this, fd]`，不是 `[&]`
- [ ] 装了 `std::thread` 的地方，析构里一定 join 了
- [ ] `submit` 用的是 `try_push`（非阻塞）而不是 `push`（阻塞）
- [ ] 拷贝构造和拷贝赋值都 `= delete`

### 测试

- [ ] 用 `check()` 不用 `assert()`
- [ ] 每个测试都**同时**验证"该发生的发生了"**和**"不该发生的没发生"
- [ ] 线程相关的测试都加了 `timeout`
- [ ] 用 TSan 跑过一遍
- [ ] 故意把代码改坏，确认测试会变红（mutation testing）

## 「能跑」和「正确」的区别

本次踩的坑里，**有一半的程序"看起来能跑"**：

| 坑 | 表面现象 | 实际 |
|---|---|---|
| 坑 9 空转 | 不崩、不报错 | 172% CPU，任务被饿死 |
| 坑 11 阻塞 submit | 所有功能测试通过 | accept 循环卡 2 秒 |
| 坑 12 少 return | 有时碰巧返回 true | 未定义行为 |
| 坑 16 atomic 标志 | 单线程测试通过 | 并发下静默 use-after-free |

**"程序没崩" ≠ "程序正确"。**

三个必备的"照妖镜"：

```bash
# 1. 警告全开（能抓住坑 12）
g++ -std=c++17 -Wall -Wextra -Wpedantic ...

# 2. TSan（能抓住坑 10）
g++ -std=c++17 -fsanitize=thread ...
setarch $(uname -m) -R ./binary      # 容器里必须加，否则 segfault

# 3. 看 CPU 和线程状态（能抓住坑 9）
top -H -p <pid>                       # 空闲时应全是 S，不是 R
```

**TSan 的容器坑**：`-fsanitize=thread` 编译出来的程序在容器里会 `FATAL: ThreadSanitizer: unexpected memory mapping` segfault，原因是 `/proc/sys/vm/mmap_rnd_bits` = 32。**必须用 `setarch $(uname -m) -R ./binary` 关掉地址随机化。**

## 一句话总结每条坑

| # | 一句话 |
|---|---|
| 1 | `std::function` 要的是**签名** `void()`，不是返回类型 `void` |
| 2 | 错误多的时候只看第一个 |
| 3 | 拼写错误编译器会告诉你，前提是你读了 |
| 4 | `std::forward` 的 `T` 不可推导，必须手写 |
| 5 | `...` 永远贴在"要展开的东西"后面；捕获列表的包捕获是 C++20 |
| 6 | `if` 后面永远加 `{}` |
| 7 | 自己用的类型自己 include |
| 8 | `catch(const std::exception&)` **抓不住 `throw 42`**，必须加 `catch(...)` |
| 9 | 空 `else` + 无限循环 = 100% CPU 空转 |
| 10 | `joinable()` 检查和 `join()` 之间有间隙 → 用 `call_once` |
| 11 | **`submit` 用阻塞 `push` = accept 循环卡死**，必须 `try_push` |
| 12 | 非 void 函数每个出口都要 `return`，否则 UB |
| 13 | `std::bind` 内部按值拷贝，完美转发全废，用 lambda + `std::apply` |
| 14 | 析构是声明顺序的倒序，`_tasks` 要写在 `_workers` 前面 |
| 15 | 捕获列表写最小集合，永远别用 `[&]` |
| 16 | "只执行一次且别人等它" 用 `call_once`，不是 `atomic<bool>` |
| 17 | 测试失败先想"断言是不是错了"，有界队列下"提交 N 个执行 N 个"是错的 |
| 18 | `assert` 在 `-DNDEBUG` 下消失，连副作用一起消失 |