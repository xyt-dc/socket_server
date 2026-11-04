# 阶段4：线程池服务端 —— 收尾 / 关闭 / 模型切换 踩坑笔记

对应文件：`src/mthread/thread_pool_server.hpp`、`src/mthread/pthread_server.hpp`、
`src/server/tcp_server.cpp`、`src/main.cpp`

前三篇的战场是"accept 失败怎么办"和"线程池本身怎么写"。
这一篇的战场是**accept 成功之后**：连接怎么收尾、进程怎么优雅关闭、
`ThreadPerConnServer` 和 `ThreadPool_server` 这两个模型到底差在哪、以及切换时踩的雷。

---

## 目录

- [零、现象速查](#零现象速查)
- [一、"只能 echo 一轮"](#一只能-echo-一轮)
  - [坑 1：收尾块写进了 while 循环体内](#坑-1收尾块写进了-while-循环体内)
  - [坑 2：`accept_once` 抛异常 → 整台状态机白写](#坑-2accept_once-抛异常--整台状态机白写)
- [二、线程池模型特有的坑](#二线程池模型特有的坑)
  - [坑 3：阻塞 recv 把 worker 焊死 → 线程池退化成"一连接一线程"](#坑-3阻塞-recv-把-worker-焊死--线程池退化成一线程一线程)
  - [坑 4：队列满回滚必须"erase + close 同一临界区"](#坑-4队列满回滚必须erase--close-同一临界区)
  - [坑 5：`cleanup_clients` 绝对不能放进 `stop()`](#坑-5cleanup_clients-绝对不能放进-stop)
  - [坑 6：`close` 唤不醒 `recv`，`shutdown` 才能](#坑-6close-唤不醒-recvshutdown-才能)
  - [坑 7：`_Pool` 必须声明在最后](#坑-7_pool-必须声明在最后)
  - [坑 8：`shutdown` 会被调用两次 —— 幸亏有 `call_once`](#坑-8shutdown-会被调用两次--幸亏有-call_once)
- [三、贯穿全局的一条不变量](#三贯穿全局的一条不变量)
- [四、并发模型切换](#四并发模型切换)
- [五、怎么验证这次改对了](#五怎么验证这次改对了)
- [六、速查表](#六速查表)

---

# 零、现象速查

| 现象 | 根因 | 坑号 |
|---|---|---|
| `nc` 发一条，收到 echo，连接**立刻被服务端断开** | 收尾块在 `while` 循环体内，`n>0` 也走了 close | 1 |
| 日志里跟着出现 `recv: Bad file descriptor` | 上一轮的 close 已经关了，下一轮 recv 打在自己身上 | 1 |
| 每来一个客户端、走一次、fd 数 +1 永不回落 | `n==0` 分支 `break` 跳过了 close | 1 |
| `Ctrl+C` 后进程**不是正常退出，是 SIGABRT / 退出码 134** | `accept_once` 里 `throw`，`classify_accept` 没机会跑 | 2 |
| 建到第 5 个连接时就卡住（4 核机器） | worker 被阻塞 recv 长期占用，任务在队列里排不上 | 3 |
| `Ctrl+C` 后进程 hang 住不退出 | 主线程 join worker，worker 卡在 recv | 6 |
| 编译/运行随机崩在析构阶段 | `_Pool` 声明顺序错，`_clients` 先被销毁 | 7 |

**退出码速查**（排查崩溃第一步）：

| 退出码 | 信号 | 含义 |
|---|---|---|
| 0 | — | 正常退出 |
| 130 | SIGINT | Ctrl+C，**信号没被处理函数吞掉** |
| 134 | SIGABRT | `std::terminate()`｜`assert` 失败｜未捕获异常 |
| 139 | SIGSEGV | 段错误 / UAF |

**看到 134 先去想"哪里抛了没接住"**，不要一上来怀疑多线程。

---

# 一、"只能 echo 一轮"

## 坑 1：收尾块写进了 while 循环体内

**症状**：`nc` 连上去发一行，回显正常，然后**连接立刻断**。再发就没反应了。

**错误写法**：

```cpp
void handle_client_read(int client_fd){
    char buf[4096];
    while (true) {
        ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
        if (n > 0) {
            if (_msg_cb) _msg_cb(client_fd, buf, n);   // ← echo 完成，然后往下掉
        } else if (n == 0) {
            break;
        } else {
            if (errno == EINTR) continue;
            perror("recv");
            break;
        }

        // ★ 问题在这：收尾块在 while 体内，n>0 时【也会执行】
        {
            std::lock_guard<std::mutex> lk(_mtx);
            _clients.erase(client_fd);
            ::close(client_fd);        // ← 回显一次就把连接关了
        }
    }
}
```

**为什么看起来"只跑一轮"**：`n > 0` 走到循环体最后，把 fd 关了；回到 `while` 顶部再 `recv`，
拿到的必然是 `EBADF`（`Bad file descriptor`）→ 走 `n < 0` 分支 → `perror` → `break`。
所以日志里应该能看到一行 `recv: Bad file descriptor`，这是**自己关自己**的铁证。

**同一处代码还制造了第二个 bug（相反方向）**：
`n == 0` 和 `n < 0` 是直接 `break` 的，**跳过了循环体尾部**，
于是正常断开和出错断开这两条路径**永远不 close、永远不 erase** → 每次断开泄漏一个 fd。

一个位置放错，同时造成"提前关"和"永不关"两种相反的错误。

**正确形状**：**循环只管收发，收尾只有一处、且在循环外**。

```cpp
void handle_client_read(int client_fd){
    char buf[4096];
    while (true) {
        ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
        if (n > 0) {
            if (_msg_cb) _msg_cb(client_fd, buf, static_cast<size_t>(n));
            continue;                       // ★ 收完继续收
        }
        if (n == 0) break;                  // 对端 FIN
        if (errno == EINTR) continue;       // 被信号打断，重试
        perror("recv");
        break;
    }

    // 收尾：唯一出口，唯一一次 close
    std::lock_guard<std::mutex> lk(_mtx);
    _clients.erase(client_fd);
    ::close(client_fd);
}
```

**修完的判据**（自己验一遍）：

- `nc` 连上去连发 5 行，**5 行都回显，连接不断**
- `Ctrl+D` / `Ctrl+C` 断开后，`ls /proc/$PID/fd | wc -l` 回落到基线

**心法**：**"资源的释放"永远只有一个出口。** 一旦你看到 `close` / `free` /
`release` 出现在多个分支里，就离双重释放或泄漏不远了。用 `break` + 循环外统一收尾，
比在每个分支里各写一遍安全得多。

---

## 坑 2：`accept_once` 抛异常 → 整台状态机白写

**症状**：`Ctrl+C` 之后进程不是打印 `server stopped` 退出，而是

```
terminate called after throwing an instance of 'std::runtime_error'
  what():  accept failed
Aborted (core dumped)
exit=134
```

**错误写法**：

```cpp
int TcpServerBase::accept_once(struct sockaddr_in &server_addr){
    socklen_t len = sizeof(server_addr);
    int accept_res = accept(_listen_fd, (struct sockaddr *)&server_addr,&len);
    if(accept_res < 0) {
        throw std::runtime_error("accept failed ");   // ← 抛了
    }
    return accept_res;
}
```

**为什么这一抛是致命的**：

`classify_accept(fd, errno)` 这套五态机（`Accept / RetryNow / Backoff / Stopped / Fatal`）
的**全部前提**是"accept 失败时返回 -1，errno 里带原因"。

一旦 `throw`，控制流**根本走不到** `classify_accept`：

```
accept 失败
  → throw
  → 穿过 run_loop 的 while（没有任何 try）
  → 穿过 start()
  → 穿过 main()
  → 栈上没有任何 catch
  → std::terminate()
  → std::abort()
  → 退出码 134
```

于是：

- `stop()` 关掉 `listen_fd` 之后那次必然失败的 `accept`（EBADF）**直接把进程炸掉**，
  精心设计的 `Stopped` 分支永远执行不到 → `cleanup_clients()` 永远不跑
- 任何一个瞬时错误（`EINTR` 被信号打断、`ECONNABORTED` 对端握手期 abort）
  都能杀掉整个服务端

**更讽刺的是**：`Stopped` 分支、指数退避、日志节流……所有这些代码写得很对，
但它们全部**建立在"accept 不抛"这个假设上**，而 `accept_once` 违反了它。

**正确写法**：只返回 -1，保留 errno，不做任何判断和打印。

```cpp
int TcpServerBase::accept_once(struct sockaddr_in& server_addr){
    socklen_t len = sizeof(server_addr);
    return accept(_listen_fd, (struct sockaddr*)&server_addr, &len);
}
```

**注意两件"不要做"的事**：

1. **不要在 `accept_once` 里 `perror`**
   阶段2 的教训：`run_loop` 已经用 `last_log` 做了「同类日志 1 秒最多 1 条」的节流。
   函数内部再打一次就绕过节流，EMFILE 退避期间照样刷屏。

2. **不要在 `accept_once` 里分类 errno**
   分层：基类只提供**无状态原语**，`classify_accept` 是**策略**，留在子类。
   这样做的好处是加一个 epoll 模型时，基类一行都不用改。

**分层原则**：**返回值信息量不足时，不要硬塞——把判断交给调用方，并在返回时保留 errno。**

---

# 二、线程池模型特有的坑

## 坑 3：阻塞 recv 把 worker 焊死 → 线程池退化成"一连接一线程"

**这是线程池模型最反直觉的地方，而且它不报错、不崩溃，性能测试也很"正常"。**

**现象**：4 核机器，`IoThreadPool` 开 4 个 worker。建到第 5 个连接时，
第 5 个客户端发数据**没有回显**——不是连接失败，是**数据卡在那里**。

**原因**：任务粒度错了。

```cpp
_Pool.submit([this, fd = res_fd.fd](){
    handle_client_read(fd);      // ← 这个函数在【整个连接生命周期内不返回】
});
```

`handle_client_read` 内部是 `while(true) recv(...)`。只要对端不关，它就一直阻塞在
`recv` 上。于是：

```
worker[0] → 连接A 的 handle_client_read → 阻塞在 recv → 直到A断开才回到队列
worker[1] → 连接B ...                                    （可能几分钟）
worker[2] → 连接C ...
worker[3] → 连接D ...
第 5 个连接的任务 → 躺在 _tasks 队列里，没人来取
```

**这时的"线程池"已经完全退化成了 `ThreadPerConnServer`，只是多了 4 这个上限。**
`BlockingQueue`、`try_push`、`submit` 返回 bool……所有这些设计都还在，
但它们要解决的"并发调度"问题根本没发生 —— 因为**根本没有"短任务"可调度**。

**这是"任务粒度"问题，不是线程池的问题。** 判断标准：

| 任务 | 适合放线程池 |
|---|---|
| `recv` 一次 → 处理 → 返回（短任务） | ✅ 完全可以 |
| `while(recv)` 直到连接断开（长任务） | ❌ 会长期占住 worker |

**要真正发挥线程池，必须把"连接"从"任务"里拆出来**，也就是**把阻塞 I/O 换成非阻塞
I/O + 事件驱动**：

```
现在的模型（每连接一个阻塞任务）：
    accept → submit(handle_client_read(fd)) → worker 阻塞在 recv

真正的线程池模型（事件驱动）：
    epoll_wait 返回可读的 fd 列表
        → 对每个就绪 fd 提交一个【短任务】：recv 一次 + 处理 + 回写
        → worker 处理完立刻回队列
```

这就是**下一阶段 `IO_server.hpp`（epoll / reactor）要解决的问题**。
`IO_server.hpp` 现在是空的，它要填的正是这个坑。

**阶段性结论**：`ThreadPool_server` 当前形态**不是终态**，它是"把并发度从 ∞ 收到 4"
的中间形态，价值在于**验证背压链路**（队列满 → `try_push` 返回 false → 拒绝连接），
而不是真的提升吞吐。想通这一点，就不会困惑于"为什么用了线程池没变快"。

---

## 坑 4：队列满回滚必须"erase + close 同一临界区"

**场景**：`submit` 返回 false（队列满或池已关），说明这个连接没人接。
此时必须**把它从 `_clients` 里摘掉并关掉**，否则 fd 泄漏。

**先看错误版本**（`erase` 和 `close` 被拆成两段，中间放了锁）：

```cpp
if(!acc){
    _clients.erase(res_fd.fd);              // ← 无锁 erase
    ++_reject_count;
    close(res_fd.fd);                       // ← 无锁 close
    std::cerr << "queue is empty" << std::endl;
}
```

**问题 1（致命）**：`erase` 和 `close` **没有锁保护**，于是和 `cleanup_clients()`
形成竞态：

```
accept 线程：_clients.erase(fd)        ← fd 已从集合里摘掉
cleanup 线程：拿到锁，遍历 _clients     ← 看不到这个 fd 了，不处理
accept 线程：close(fd)                 ← 关闭
accept 线程：accept 拿到新连接 → 复用同一个 fd 号
cleanup 线程：…… 这时如果它拿到的是【旧快照】就去 shutdown → 关掉新连接
```

**问题 2（次要）**：`std::cerr <<` 放在临界区里。`iostream` 有自己的内部锁，
还要分配内存，持锁期间做 I/O 是典型的性能自伤 —— 而且这时候本来就过载了。

**问题 3（纯文案）**：`submit` 返回 false 的意思是**队列满**（`try_push` 满了）
或者**池子已关闭**，跟 "empty" 正好相反。

**正确写法**（`erase` + `close` 同一临界区，I/O 移出）：

```cpp
bool acc = _Pool.submit([this, fd = res_fd.fd](){ handle_client_read(fd); });
if (!acc) {
    {
        std::lock_guard<std::mutex> lk(_mtx);
        _clients.erase(res_fd.fd);             // ① 摘除登记
        ::close(res_fd.fd);                    // ② 释放 fd（与 ① 不可拆）
        ++_reject_count;                       // ③ atomic，放里放外都行
    }
    std::cerr << "[reject] queue full, fd=" << res_fd.fd
              << " total=" << _reject_count.load() << std::endl;   // 锁外打印
}
```

**为什么 `close` 必须在锁内**：见[第三节的不变量](#三贯穿全局的一条不变量)。
一句话——**只要"fd 还开着"和"fd 在 `_clients` 里"这两个状态不同步，
`cleanup_clients` 就可能关错 fd**。

**顺带**：这是**背压（backpressure）**的落点。整个链路是

```
队列满 → try_push 返回 false → 拒绝新连接（close fd）→ 客户端重连
```

而不是像 `push` 那样"睡到有空间"（阶段3 坑 11，把 accept 循环卡死 2 秒）。
**过载时宁可明确拒绝，也不能拖慢 accept 循环。**

---

## 坑 5：`cleanup_clients` 绝对不能放进 `stop()`

**这是我最想写下来的一条，因为它"看起来完全合理"。**

`stop()` 的职责是"让 `start()` 返回"，`cleanup_clients()` 的职责是"收尸"。
直觉上这两件事应该一起做，但**技术上做不到**。

**原因：`stop()` 是跑在信号处理函数里的。**

```cpp
void on_signal(int) { g_server->stop(); }   // ← 信号上下文
```

**信号处理函数里只允许调用异步信号安全的函数**（POSIX 定义的，`man 7 signal-safety` 有完整列表）。
`cleanup_clients` 里用了什么：

| 操作 | 信号安全？ | 后果 |
|---|---|---|
| `std::mutex::lock` / `lock_guard` | ❌ | 如果主流程正好持锁，**死锁**（信号打断了持锁者） |
| `std::thread::join()` | ❌ | 内部要 `futex_wait`，同样死锁 |
| `std::cerr << ...` | ❌ | 要分配内存 + 持 `iostream` 内部锁 |
| `std::call_once` | ❌ | 内部用 mutex |
| `::close(fd)` | ✅ | OK |
| `_running = false`（atomic store） | 标准上不保证，实践上安全 | 可用 |

**这就是死锁的完整链条**：

```
主线程：_mtx.lock()  ← 正在 accept 循环里插 _clients
        ↑ 信号到达
信号处理函数：stop() → cleanup_clients() → _mtx.lock()   ← 永远等不到
```

**正确做法：把职责切成两半，用"错误"当通信手段。**

```cpp
void stop(){                     // 只做异步信号安全的两件事
    _running = false;
    if (_listen_fd >= 0) { ::close(_listen_fd); _listen_fd = -1; }
}
```

然后 `cleanup_clients()` 放在 **`run_loop` 的末尾**（**普通线程上下文**）：

```cpp
void run_loop() override {
    while (_running) {
        int fd = accept_once(client_addr);
        // ... 五态机 ...
        else if (status == AcceptStatus::Stopped) break;   // ← close 了 listen_fd 后走这里
    }
    cleanup_clients();               // ★ 这里是普通线程，锁和 join 都安全
}
```

**完整链路**（这条链要背下来）：

```
SIGINT
  → stop()                        [信号上下文，只置标志 + close]
  → _running = false
  → close(_listen_fd)
  → 阻塞中的 accept 立刻返回 -1，errno = EBADF
  → classify_accept → AcceptStatus::Stopped
  → break 出 while
  → run_loop 末尾 cleanup_clients()   [普通线程，随便加锁 join]
  → start() 返回
  → main() 返回 → 析构
```

**心法**：**信号处理函数是"通知"，不是"执行"。**
它只该改一个标志（或写一个 `eventfd` / `pipe`），真正的活儿交给主循环。
所有"在 signal handler 里关了 fd 啊、释放了资源啊"的写法，都是在给自己埋雷。

---

## 坑 6：`close` 唤不醒 `recv`，`shutdown` 才能

**背景**：优雅关闭时，worker 卡在 `recv` 上，主线程要它赶紧结束。
直觉做法是 `close(fd)` —— **没用**。

**为什么 close 不行**：

`close(fd)` 只是**把本进程对那个 file 对象的引用计数减一**。
如果还有别的线程正在这个 fd 上阻塞（`recv` 内部持有 file 引用），
引用计数没归零 → 内核不会释放 → **阻塞的 `recv` 继续睡**。

而 `shutdown(fd, SHUT_RDWR)` 是**直接对 socket 下达"关闭两个方向"的命令**：

```
本端 SHUT_RD  → 阻塞中的 recv 立刻返回 0（EOF）
本端 SHUT_WR  → 对端收到 FIN
对端也关了     → recv 返回 0
```

于是 worker 的 `recv` 返回 0 → `break` → 走到循环外的收尾块 → `close(fd)`。

**所有权分工（关键）**：

| 谁 | 做什么 | 为什么 |
|---|---|---|
| `cleanup_clients()`（主线程） | 只 `shutdown`，**不 close** | 只负责"叫醒" |
| `handle_client_read()`（worker） | 只 `close`，**不 shutdown** | 只负责"释放" |

```cpp
void cleanup_clients(){
    {
        std::lock_guard<std::mutex> lk(_mtx);
        for (int fd : _clients) {
            ::shutdown(fd, SHUT_RDWR);      // ← 只叫醒，不关
        }
    }
    _Pool.shutdown();                        // ← 等 worker 自己收尾退出
}
```

**为什么不能两边都 close**：`close` 两次同一个 fd，第二次时这个号**可能已经被
`accept` 复用给新连接**了 —— 你等于顺手把无辜的连接杀掉
（`note_stage2_accept错误处理与fd耗尽.md` 的「问题 6」是同一个雷）。

**心法**：**谁分配，谁释放。** fd 是 accept 循环分配的，但"活着的 fd"这个概念
交给了 worker（因为它拿着 `client_fd`），所以 `close` 归 worker。
清理方只能"请求"，不能"处决"。

---

## 坑 7：`_Pool` 必须声明在最后

**C++ 的析构顺序是声明顺序的倒序。**

```cpp
class ThreadPool_server : public TcpServerBase {
    std::unordered_set<int> _clients;   // ① 先声明 → 最后析构
    std::mutex              _mtx;       // ②
    std::atomic<uint64_t>   _reject_count{0};
    IoThreadPool            _Pool;      // ④ 最后声明 → 最先析构 ✅
};
```

**如果 `_Pool` 写在 `_clients` 前面**（错误的顺序）：

```
析构开始
  → _clients 先析构（unordered_set 的桶全被释放）
  → 但 _Pool 的 worker 还在跑！
  → worker 里的 handle_client_read 执行 _clients.erase(fd)
  → 访问已释放的内存 → UAF → 随机崩溃
```

而且这种崩溃**不一定会立刻出现**：默认析构函数体（`~ThreadPool_server`）
里调了 `_Pool.shutdown()`，join 完 worker 才走到成员析构 —— 所以靠**析构函数体**
兜住了。

但这只是运气：**如果哪天把 `_Pool.shutdown()` 从析构函数里删掉**
（"反正 `IoThreadPool` 自己的析构会 shutdown"），顺序错了就立刻炸。

**这就是"声明顺序"的意义**：它是一层**不依赖析构函数体写法的安全网**。

**背下来**：

```
构造：从上到下
析构：从下到上
```

**依赖别人的成员，声明在后面**（先析构）；**被依赖的成员，声明在前面**（后析构）。

这条在 `note_stage3_线程池踩坑.md` 坑 14 里写过（`_tasks` 要在 `_workers` 前），
这里换了场景又犯一次 —— 说明它不是"某一个类的问题"，而是一条通用规则。

---

## 坑 8：`shutdown` 会被调用两次 —— 幸亏有 `call_once`

现在 `_Pool.shutdown()` 有两个调用点：

1. `run_loop` 末尾 → `cleanup_clients()` 里
2. `~ThreadPool_server()` 里

**如果 `IoThreadPool::shutdown` 不是幂等的**（阶段3 坑 10），
第二次 `join` 一个已经 join 过的 thread → **未定义行为 / `terminate`**。

因为当时用了 `std::call_once`，第二次调用是**空操作**，所以这里安全：

```cpp
void shutdown() {
    std::call_once(_once, [this] {          // ← 第二次调用直接返回
        _tasks.close();
        for (auto& w : _workers) if (w.joinable()) w.join();
        _workers.clear();
    });
}
```

**这条经验的价值**：**幂等性不是"洁癖"，是"允许防御性重复调用"。**
正因为 `shutdown` 幂等，我们才敢在 `cleanup_clients`（正常路径）
**和** 析构函数（异常路径、提前 return 路径）**两个地方都调它**，
而不用担心"哪条路径会先跑"。

**反过来说**：如果你写的任何"清理函数"需要调用方去记住"只能调一次"，
那就是设计缺陷。让它幂等，调用方就可以无脑调。

---

# 三、贯穿全局的一条不变量

整个 `ThreadPool_server` 的正确性，压在这一条上：

> **fd 还活着 ⟺ fd 在 `_clients` 里。**

任何破坏它的写法都会引出 bug：

| 破坏方式 | 后果 |
|---|---|
| `emplace` 有锁、`erase` 无锁 | `cleanup_clients` 看到已经关掉的 fd → `shutdown` 一个已被复用的号 → 关掉别人的连接 |
| `erase` 完放锁**再** `close` | 中间窗口：`cleanup_clients` 拿到快照含此 fd，去 `shutdown`，而这边已 `close` |
| `break` 跳过 `erase`+`close` | fd 泄漏（坑 1 里的第二个 bug） |
| 两处都 `close` | 双重 close / 关错连接 |

**所以正确的写法是**：`erase` 和 `close` 在**同一把锁**里，且**只有一处**。

```cpp
// worker 侧：唯一收尾出口
{
    std::lock_guard<std::mutex> lk(_mtx);
    _clients.erase(client_fd);
    ::close(client_fd);              // 同一临界区，不可拆
}

// cleanup 侧：只读快照，只 shutdown，绝不 close
{
    std::lock_guard<std::mutex> lk(_mtx);
    for (int fd : _clients) ::shutdown(fd, SHUT_RDWR);
}
```

**顺带一个性能上的自觉**：这把锁在 accept 循环（插）、每个 worker（删）、
cleanup（遍历）三处会被抢。当前 worker 数只有 4，**争用可以忽略**。
但如果以后做到几百 worker，就该换成
"每条连接一个 `shared_ptr<Connection>` + 引用计数"，
而不是继续拿一把全局锁保护一个 `unordered_set<int>`。

**锁的粒度随手写，等到压测再改 —— 但要知道它会在哪儿成为瓶颈。**

---

# 四、并发模型切换

三个模型都在 `src/mthread/` 下，共享同一个基类接口：

```cpp
TcpServerBase              // src/server/tcp_server.hpp
    start()                // 固定流程 → run_loop()
    stop()                 // _running=false + close(listen_fd)
    set_message_callback() // 业务注入点
    run_loop() = 0         // ★ 唯一的差异点
```

| 模型 | 文件 | `run_loop` 的做法 | 并发度 |
|---|---|---|---|
| 串行 | （尚未写，`run_loop` 里直接 `handle_client_read`） | accept 后当场处理 | 1 |
| 一连接一线程 | `pthread_server.hpp` | accept 后 `std::thread(...).detach()` | ∞（无上限） |
| 线程池 | `thread_pool_server.hpp` | accept 后 `_Pool.submit(...)` | 硬上限 = worker 数 |
| epoll / reactor | `IO_server.hpp`（空） | `epoll_wait` + 非阻塞 I/O | 事件驱动 |

## 切换方式

**方式 A：改两行（当前 `main.cpp` 用的）**

```cpp
#include "mthread/pthread_server.hpp"        // ← 换 include
// #include "mthread/thread_pool_server.hpp"

ThreadPerConnServer server(port);            // ← 换类型
// ThreadPool_server server(port);
```

然后 `cmake --build build -j`。

**方式 B：三个 `add_executable`（推荐做对比压测时用）**

```cmake
foreach(kind base thread pool)
    add_executable(server_${kind} src/main.cpp src/server/tcp_server.cpp)
    target_compile_definitions(server_${kind} PRIVATE SERVER_KIND_${kind})
endforeach()
```

**方式 C：运行时 `--server=` 参数**

`std::unique_ptr<TcpServerBase>` + 一小段 `argv` 解析，
一个二进制跑所有模型，**对比时编译选项完全一致，不会混进噪声**。

## 切换时的三个注意点

**① `TcpServerBase` 是抽象类，不能直接实例化**

`run_loop()` 是纯虚函数。想跑"串行"基线，必须写一个最小派生类
（放 `src/mthread/base_server.hpp`），它属于**并发模型**，不是测试代码。

**② `stop()` 不是虚函数，但可以安全地通过基类指针调用**

因为 `stop()` 只碰基类成员（`_running` / `_listen_fd`）。
`g_server` 声明成 `TcpServerBase*` 是对的：

```cpp
TcpServerBase* g_server = nullptr;      // ✅ 基类指针，能指向任意模型
ThreadPool_server server(port);          // 具体模型
g_server = &server;
```

反例：如果写成 `ThreadPool_server* g_server`，那么切到
`ThreadPerConnServer` 时就会编译失败（`ThreadPerConnServer*` 不能转成
`ThreadPool_server*`）—— 这正是我中途遇到过的一个编译错误。

**③ `pthread_server.hpp` 的 `detach()` 是已知缺陷，不是写法**

文件头注释里列了四条：线程数无上限 / `detach` + 捕获 `this` = UAF /
`stop()` 唤不醒 `recv` / 无法优雅关闭。
它是**用来对比的反面教材**，跑压测时会直接暴露这些问题。

---

# 五、怎么验证这次改对了

## 1. 单点功能：多轮 echo

```bash
./build/socket_server 6003 &
{ echo hello; echo world; echo third; } | nc -N 127.0.0.1 6003
```

**通过判据**：三行全部回显，且服务端没有
`client ... closed` 之外的错误日志。

## 2. fd 不泄漏

```bash
PID=$(pgrep -f 'socket_server 6003')
ls /proc/$PID/fd | wc -l            # 基线
# ... 跑 50 次连接 ...
ls /proc/$PID/fd | wc -l            # 必须回到基线
```

## 3. 优雅关闭

```bash
kill -INT $PID
echo "exit=$?"                       # 必须 0，不能是 134 或 130
```

**通过判据**：

- 打印 `server stopped`
- 退出码 0（**不是 134** —— 那是坑 2 又回来了）
- 进程**立刻**退出，不 hang（hang 说明坑 6 没做对）

## 4. 背压生效

```bash
# 队列 cap 设成 2，4 个 worker 全被长连接占住，再灌连接
./build/socket_server 6003
# 另开 10 个 nc 都发一行但不关
```

**通过判据**：日志出现 `[reject] queue full`，`_reject_count` 增长，
服务端不 OOM、accept 循环不卡。

> **注意**：这个测试在 [坑 3](#坑-3阻塞-recv-把-worker-焊死--线程池退化成一线程一线程)
> 描述的模型下**很容易通过**（因为 worker 很快就被焊死了），
> 所以它验的是"背压链路通不通"，**不是**"线程池性能好不好"。

---

# 六、速查表

## 坑点一句话

| # | 一句话 |
|---|---|
| 1 | 收尾块放循环体内 = 提前关；放错位置同时造成"提前关"和"永不关"两种 bug |
| 2 | `accept_once` 里 `throw` → 五态机白写，Ctrl+C 变 134 |
| 3 | 阻塞 `recv` 当任务 = worker 被焊死，线程池退化成"一连接一线程 + 上限 4" |
| 4 | 队列满回滚的 `erase` + `close` 必须在同一把锁里一次做完 |
| 5 | 信号处理函数只发通知，不执行清理；cleanup 放 `run_loop` 末尾 |
| 6 | `close` 唤不醒 `recv`，`shutdown(SHUT_RDWR)` 才能；清理方只 shutdown，worker 才 close |
| 7 | `_Pool` 声明在最后（最先析构），否则 worker 访问已销毁的 `_clients` |
| 8 | `shutdown` 幂等 → 才敢在 cleanup 和析构两处都调 |

## 三个"只做一件事"的分工

```
stop()                只置标志 + close(listen_fd)      信号安全
run_loop 末尾          调 cleanup_clients()              普通线程
cleanup_clients()     只 shutdown + _Pool.shutdown()     不 close
handle_client_read()  只 close + erase                   不 shutdown
```

## 排查命令

```bash
# 退出码 → 信号
./build/socket_server 6003; echo "exit=$?"

# fd 泄漏
ls /proc/$(pgrep -f socket_server)/fd | wc -l

# 线程状态（空闲时应该全是 S，不是 R）
top -H -p $(pgrep -f socket_server)

# 端口状态（监听/残留）
ss -tlnp sport = :6003
ss -tn   sport = :6003

# 卡在哪个系统调用
strace -f -p $(pgrep -f socket_server) -e trace=network,close
```

---

## 遗留问题（留给阶段5 / epoll）

- [ ] 坑 3 的根治：把 `recv` 拆成非阻塞 + `epoll_wait`，让任务变成"短任务"
- [ ] `_clients` 换成 `shared_ptr<Connection>` + 引用计数，去掉全局锁
- [ ] `Fatal` 分支缺连续次数上限（连续 100 次永久错误应该停服）
- [ ] `_idle_fd` 预留技巧要不要在线程池模型里恢复（EMFILE 时踢一个连接腾 fd）
- [ ] `_listen_fd` 改成 `std::atomic<int>` + `exchange(-1)` 彻底解决双重 close