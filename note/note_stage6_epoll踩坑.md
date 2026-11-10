# 阶段6：epoll 踩坑全记录 —— 从三原语封装到写路径闭合

对应文件：`lib/epoll.hpp`、`lib/Connection.hpp`、`src/mthread/Epoll_server.hpp`、
`test/test_04_epoll.sh`、`test/manual_check_wbuf.py`、`test/bigfile_echo.py`

前五篇记的是"怎么把线程模型写对"，以及"为什么线程模型必然失效"。
这一篇记的是**把 reactor 真正写出来时，每一个能让它崩、卡、丢数据、假绿的地方**。

这一篇的坑分两类：

- **显性的**：写错就编译不过、或者立刻崩。好抓。
- **隐性的**：写错之后**测试全绿、日志干净、进程活着**，但数据已经悄悄丢了。
  这一类占了本阶段 80%，也是整篇的重点。

所有结论有实测支撑的标注 `【实测】`，靠推理的标注 `【推算】`。

---

## 目录

- [零、结论先行](#零结论先行)
- [一、epoll 三原语封装](#一epoll-三原语封装)
- [二、listen fd 与 accept](#二listen-fd-与-accept)
- [三、事件分发：run_loop 的四个必答题](#三事件分发run_loop-的四个必答题)
- [四、读路径](#四读路径)
- [五、写路径：本阶段最大的坑区](#五写路径本阶段最大的坑区)
- [六、生命周期：double close 与空指针](#六生命周期double-close-与空指针)
- [七、调试方法论：怎么把"看起来对"证伪](#七调试方法论怎么把看起来对证伪)
- [八、假绿识别手册](#八假绿识别手册)
- [九、坑位总表](#九坑位总表)

---

# 零、结论先行

五句话，都是付了代价换来的：

1. **epoll 本身不难，难的是"非阻塞 I/O 的半成品状态"。**
   `recv` 可能只给一半、`send` 可能只收一半，你要为"另一半"找地方存、
   找时机续、找条件清。**这三件事各有一个坑。**

2. **`EPOLLOUT` 是水平触发（LT）模式下最危险的事件。**
   它不像 `EPOLLIN` 那样"没数据就不响"——只要内核发送缓冲区有空位它就**永远响**。
   注册了不摘，`epoll_wait` 立刻返回，`while` 变成 100% CPU 忙等。`【实测】`

3. **最贵的 bug 是"测试全绿但数据丢了"。**
   `test_04` 阶段 [6] 用 64 B 小包压测，**永远撞不到 `EAGAIN`**
   （默认 `tcp_wmem` 就有 16 KB），所以 `handle_write` 写没写都绿。
   这个绿是假绿，掩盖了一个丢失 76% 数据的真实缺陷。`【实测】`

4. **"消费缓冲区"和"摘除 EPOLLOUT"是同一件事的两半，不能只修一半。**
   只修消费 → 数据对了但 CPU 100%；
   只修摘除 → CPU 对了但数据还是丢。
   两个必须一起改。`【实测】`

5. **`epoll_ctl(DEL)` 必须在 `close` 之前。**
   顺序反了不会报错，但 `fd` 已经被内核回收、可能立刻被新连接复用，
   你删掉的是**新连接**的监视。这是最典型的"静默失效"。

---

# 一、epoll 三原语封装

`lib/epoll.hpp` 把 `epoll_create1` / `epoll_ctl` / `epoll_wait` 包成 RAII 类。
表面简单，坑不少。

## 1.1 `epoll_create1` 必须带 `EPOLL_CLOEXEC`

```cpp
_epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
```

不带的话，`fork` + `exec` 出来的子进程会继承这个 fd。
单进程程序看不出问题，一旦引入 `system()` / `posix_spawn` / 任何 subprocess，
epoll 实例就泄漏到子进程里，且子进程永远不知道它是什么。

同理，`accept4` 要用 `SOCK_CLOEXEC`，`socket()` 要用 `SOCK_CLOEXEC`。
**在 Linux 上，"四件套"漏一个就是泄漏源**：`epoll_create1(EPOLL_CLOEXEC)` +
`accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)` + `socket(SOCK_CLOEXEC)` + `open(O_CLOEXEC)`。

## 1.2 `epoll_event.data` 不设置，`wait` 返回的就是垃圾

```cpp
epoll_event ev{};
ev.events  = events;
ev.data.fd = fd;      // ★ 漏了这行，wait 之后 event_poll[i].data.fd 是随机值
```

`epoll_data_t` 是个 union（`ptr` / `fd` / `u32` / `u64`），
内核不碰它，**原样回传**。你不填，`epoll_wait` 返回的就是栈上的垃圾值，
然后 `handle_read(垃圾 fd)`。

这个坑的表现非常迷惑：所有连接都正常，但偶尔收到一个 `fd = 139463281` 的事件，
`_conns.find()` 找不到直接返回，或者更糟——恰好撞上某个真实 fd 号，
去操作一条完全无关的连接。

**用 `ptr` 而不是 `fd` 还能顺便省掉一次 hash 查找**（直接回传 `Connection*`），
但在 `shared_ptr` 方案下需要额外处理生命周期，暂用 `fd` 更简单。

## 1.3 `epoll_wait` 的 `maxevents` 必须 ≤ 缓冲区容量

```cpp
if(active_events.empty()){
    active_events.resize(static_cast<size_t>(_max_events));   // ★ 必须补齐
}
const int maxev = static_cast<int>(
    std::min(active_events.size(), static_cast<size_t>(_max_events)));
::epoll_wait(_epoll_fd, active_events.data(), maxev, timeout_ms);
```

两个坑叠在一起：

- **`vector` 只 `reserve` 不 `resize`，`data()` 返回的是未初始化内存**，
  且 `size()` 为 0。把 `data()` 交给内核写 1024 个元素 = 越界写。
  本项目的 `EpollServer::epoll_init()` 里只调了 `event_poll.reserve(kMaxEvents)`，
  幸好 `wait()` 内部有 `resize` 兜底，否则第一次调用就是堆破坏。
  **这个"兜底"是我写的，不是设计出来的——说明这个坑真实存在。**

- **`maxevents` 大于缓冲区容量 = 内核越界写**。取 `min` 是唯一安全写法。

## 1.4 `epoll_wait` 返回后只能遍历前 `n` 个

```cpp
// ★ 清零尾部：上次跑的比这次多时，残留的旧事件会被误当成本轮有效事件
for(int i = ready_count; i < maxev; ++i){
    active_events[static_cast<size_t>(i)] = epoll_event{};
}
```

如果调用方写的是 `for (auto& ev : event_poll)`，而不是 `for (int i=0; i<n; ++i)`，
就会把上一轮残留的事件重新处理一遍。表现是"消息被重复处理"或"操作已关闭的 fd"。

本项目的 `run_loop()` 用的是 `for(int i = 0; i < n; ++i)`，**写对了**。
但 `Epoll::wait()` 里仍然做了尾部清零，属于**双保险**——因为它是个通用组件，
不能假设调用方一定用对。

## 1.5 `EINTR` 必须重试

```cpp
for(;;){
    const int ready_count = ::epoll_wait(...);
    if(ready_count < 0){
        if(errno == EINTR){ continue; }   // ★ 信号打断，重试
        return -1;
    }
    ...
}
```

`epoll_wait` 被信号打断返回 `-1 / EINTR`。不重试的话，
`run_loop` 里的 `if(n < 0) break;` 会让**整个服务器因为一个 SIGCHLD / SIGWINCH 就退出**。

## 1.6 `mod` 的 `||` 笔误（真实发生过的）

```cpp
bool mod(int fd, uint32_t events = EPOLLIN|EPOLLOUT);    // ✅ 按位或
bool mod(int fd, uint32_t events = EPOLLIN||EPOLLOUT);   // ❌ 逻辑或 = 1
```

`EPOLLIN` 是 `0x001`，`EPOLLOUT` 是 `0x004`。
`EPOLLIN || EPOLLOUT` 求值为 `true` → 转成 `uint32_t` 就是 `1` → 只有 `EPOLLIN`。
**能编译、能运行、看起来完全正常，只是 `EPOLLOUT` 永远不触发。**

这个坑的修复成本是 1 个字符，但发现成本是：写路径完全失效，且没有任何报错。
`-Wall -Wextra` 对它一点反应都没有。

## 1.7 移动构造必须 `noexcept`

```cpp
// ⚠️ 移动赋值必须 noexcept，否则 vector<Epoll> 扩容时优先走拷贝
//    （拷贝已被 = delete）→ 直接编译失败
Epoll(Epoll&& other) noexcept { ... }
```

`std::vector` 扩容时只有 `noexcept` 的移动构造才会被选中，
否则它为了强异常保证会退回去用拷贝——而拷贝已经被 `= delete`，
于是编译期报一堆看不懂的模板错误。

## 1.8 `max_events` 构造值与 `set_max_events` 的错配

```cpp
_epoll = std::make_unique<Epoll>(EPOLL_CLOEXEC, 64);   // 构造时 64
...
_epoll.get()->set_max_events(kMaxEvents);              // run_loop 里改成 1024
```

如果哪天有人忘了在 `run_loop` 里调 `set_max_events`，
那么**每次 `epoll_wait` 最多只能拿回 64 个事件**。
1000 个连接同时就绪时，你要跑 16 轮 `epoll_wait` 才能处理完一轮。
这不丢数据（LT 会重复通知），但延迟会莫名其妙地高，且极难定位。

**原则：构造参数的默认值应该是"安全上限"，而不是"我随手写的数"。**
这里 64 就是随手写的。

---

# 二、listen fd 与 accept

## 2.1 `listen_fd` 必须是非阻塞的

```cpp
int flags = fcntl(_listen_fd, F_GETFL);
fcntl(_listen_fd, F_SETFL, flags | O_NONBLOCK);
```

**为什么非阻塞是必须的**，而不是"最好"：

`handle_accept()` 是个 **drain 循环**——一直 `accept` 到 `EAGAIN` 才退出：

```cpp
for(;;){
    int cfd = ::accept4(_listen_fd, nullptr, nullptr, SOCK_NONBLOCK|SOCK_CLOEXEC);
    if(cfd < 0){
        if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }   // 队列空了，正常退出
        ...
    }
}
```

**阻塞 fd 上永远不会有 `EAGAIN`。** 队列空的时候 `accept` 会**永久阻塞**，
整个 reactor 卡死在 `handle_accept` 里，所有其他连接的事件全部停止处理。

表现：服务器看起来活着（进程在、端口在 listen、`/proc` 里 fd 都在），
但**没有任何响应**，CPU 0%。这比崩溃难查一百倍。

### 一个必须澄清的误解

> "listen fd 阻塞的话系统会崩"

**不会崩。** 它只会让 reactor 静默死锁。这个区分很重要，因为它决定了你排查方向：

| 现象 | 真实原因 |
|---|---|
| 进程消失 / 退出码非 0 | 崩了，去看 core / gdb |
| 进程在、CPU 0%、但无响应 | **卡住了**，去看是不是某个阻塞调用 |
| 进程在、CPU 100% | **忙等**，去看是不是 EPOLLOUT 没摘 / accept 失败没退避 |

`listen fd` 阻塞 + drain 循环 = 第二类（卡住）。
`listen fd` 非阻塞但忘了注册 `EPOLLOUT` 的摘除 = 第三类（忙等）。

**记住这个三分法，90% 的 epoll 故障能被归到某一类，然后直接定位。**

## 2.2 新连接的 fd 也必须非阻塞

```cpp
int cfd = ::accept4(_listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
//                                          ^^^^^^^^^^^^^^ ★ 关键
```

**`accept` 返回的 fd 不继承 `listen_fd` 的 `O_NONBLOCK`。**
这是最容易忘的一条——`listen_fd` 改好了，新连接全是阻塞的。

后果：`handle_read` 里 `recv` 一旦没数据就永久阻塞，整个 reactor 又卡死了。
而且这个 bug **只在"连上了但暂时没数据"时触发**，功能测试（连上就发）完全测不出来。

这就是为什么必须用 `accept4` 而不是 `accept`：`accept4` 能原子地带上
`SOCK_NONBLOCK | SOCK_CLOEXEC`，避免了
`accept` → `fcntl` 之间的竞争窗口（那个窗口里 fd 是阻塞的）。

## 2.3 backlog 从 5 改成 128

```cpp
int listen_res = listen(_listen_fd, 128);
```

`test_04_epoll.sh` 阶段 [4] 要 500 条连接，实测只成功 **480** 条：

```
[4] 空闲连接 500 连接只连不发
    conn_ok=480 / 需 500                              FAIL
```

改 backlog 为 128 后通过。

**为什么 5 会失败**：`listen` 的 `backlog` 是**已完成三次握手、等待 `accept` 的队列长度**。
服务端还没来得及 `accept` 时，超出的连接会被内核丢弃（或按
`tcp_abort_on_overflow` 发 RST）。客户端看到的是"连接被重置"或超时。

注意：Linux 会把 `backlog` 按 `somaxconn` 截断，
所以实际值是 `min(参数, /proc/sys/net/core/somaxconn)`。`【推算】`

```bash
$ cat /proc/sys/net/core/somaxconn
4096
```

所以写 128 实际生效 128，足够 500 条连接的突发。

## 2.4 `EMFILE` 必须单独处理

```cpp
if (errno == EMFILE || errno == ENFILE) {
    std::cerr << "fd exhausted: " << std::strerror(errno) << '\n';
    break;      // ★ 必须 break，不能 continue
}
```

**`EMFILE` 时 `continue` = 无限忙等 + 每秒几十万行日志。**
因为 fd 没被释放，下一次 `accept` 还是 `EMFILE`。
这个坑在 `note_stage2` 里已经付出过一次代价（CPU 100%、580,941 行 / 2s）。

## 2.5 `accept` 的"惊群"与空返回

`accept4` 返回 `-1 / EAGAIN` 是**正常情况**（队列在 `epoll_wait` 和 `accept` 之间被
其他线程抢空了）。它不代表错误，不能打日志、不能 `del`。

而 `-1 / EINTR` 是信号打断，应该 `continue`。
两者必须分开处理，否则日志会被 `EAGAIN` 刷爆。

---

# 三、事件分发：run_loop 的四个必答题

```cpp
for(int i = 0; i < n; ++i){
    int fd = event_poll[i].data.fd;
    const uint32_t ev = event_poll[i].events;

    if (fd == _listen_fd) { handle_accept(); continue; }

    if (ev & EPOLLERR) { del_and_close(fd); continue; }

    if (ev & EPOLLHUP) {
        if (ev & EPOLLIN) handle_read(fd);
        else              del_and_close(fd);
        continue;
    }

    if (ev & EPOLLIN)  handle_read(fd);
    if (ev & EPOLLOUT) handle_write(fd);
}
```

这 15 行里有四个必须答对的问题，答错任何一个都会造成"静默故障"。

## 3.1 为什么 `fd == _listen_fd` 必须第一个判断

`listen_fd` 也会产生 `EPOLLIN`（有新连接），如果走到 `handle_read(listen_fd)`，
`recv` 一个处于 listen 状态的 fd 会返回 `-1 / ENOTCONN`，然后 `del_and_close` 掉
**监听 fd 本身**——服务器从此再也不接受新连接，但进程还活着、端口还在。

实测会看到：`epoll_init` 之后第一个连接进来，服务器就死了。

## 3.2 为什么 `EPOLLERR` / `EPOLLHUP` 必须优先于 `EPOLLIN`

**这是 LT 模式最阴的坑。** 内核的规定是：

> 即使你不注册 `EPOLLERR` / `EPOLLHUP`，它们也会被**无条件**报告。

也就是说，一个连接出错（`ECONNRESET`）时，`epoll_wait` 返回的 `events`
可能是 `EPOLLIN | EPOLLERR | EPOLLHUP` 的组合。

如果代码写成：

```cpp
if (ev & EPOLLIN)  handle_read(fd);      // 先走这个分支
else if (ev & EPOLLERR) del_and_close(fd);   // 永远到不了
```

那么 `handle_read` 里 `recv` 返回 `-1 / ECONNRESET` → `del_and_close` → 看起来也没问题。
但如果 `handle_read` 因为某些原因没有关闭连接（比如 `recv` 返回了残留数据），
那么下一轮 `epoll_wait` **立刻再次返回同一个 `EPOLLERR`**，
因为错误状态在 fd 上一直存在（LT 模式的本质）→ **100% CPU 忙等**。

**把 `EPOLLERR` / `EPOLLHUP` 放在最前面，是唯一能保证"错误状态一定被清除"的写法。**

## 3.3 为什么 `EPOLLHUP` 要先读一次再关闭

```cpp
if (ev & EPOLLHUP) {
    if (ev & EPOLLIN) handle_read(fd);   // ★ 先榨干残留数据
    else              del_and_close(fd);
    continue;
}
```

对端 `close()` 后，内核里可能还有**未读完的数据**。
直接 `del_and_close` 会丢掉最后一批数据。

典型场景：客户端 `send("bye")` 然后 `close()`。
如果服务端看到 `EPOLLHUP` 就直接关，`"bye"` 永远处理不到。

而 `handle_read` 内部读到 `n == 0` 时会自己 `del_and_close`，
所以把它放这里既能读数据、又能正确关闭，**不需要额外分支**。

## 3.4 为什么 `EPOLLIN` 和 `EPOLLOUT` 是两个独立 `if`

```cpp
if (ev & EPOLLIN)  handle_read(fd);
if (ev & EPOLLOUT) handle_write(fd);     // ★ 不是 else if
```

同一个 fd 完全可能"同时可读可写"——这正是 `EPOLLIN|EPOLLOUT` 都注册时最常见的返回。

写成 `else if` 的后果：**写事件永远被读事件饿死**。
如果客户端持续快速发送数据（`EPOLLIN` 一直就绪），`handle_write` 永远没机会跑，
`wbuf` 只增不减，内存持续膨胀，最终 OOM。

这个 bug 在小包测试里完全看不出来（`EPOLLIN` 和 `EPOLLOUT` 很少同时就绪）。

## 3.5 一个不易察觉的 use-after-reuse 隐患

```cpp
if (ev & EPOLLIN)  handle_read(fd);      // 这里可能 del_and_close(fd)
if (ev & EPOLLOUT) handle_write(fd);     // 这里再去查 _conns
```

如果 `handle_read` 里关闭了连接，`handle_write(fd)` 会去 `_conns.find(fd)`
查不到而直接 `return` —— **当前是安全的**。

但这个安全性依赖两个前提：

1. `handle_write` 开头必须查 `_conns`（用户写对了）
2. **同一个 `for` 循环内不会 `accept` 新连接**

第二个前提是隐含的：`accept` 只在 `fd == _listen_fd` 分支发生，
一轮循环里最多处理一次，所以刚关闭的 fd 号不可能在这一轮内被复用。

**一旦把 `IoThreadPool` 接回来（`try_run_one()` 那条待办），
前提 2 就会被打破**——worker 线程可能在主线程处理 `EPOLLOUT` 时 `accept` 出
一个恰好复用了刚才 fd 号的新连接，于是 `handle_write` 拿到的是**新连接**，
把旧连接的 `wbuf` 数据发给新客户端。

这是一个真正的跨线程 use-after-reuse，**目前不会发生，但改造时必须处理**。

---

# 四、读路径

## 4.1 `recv` 返回 0 = 对端 FIN，必须关闭

```cpp
}else if(n == 0){
    del_and_close(fd);
    return;
}
```

**漏掉这个分支 = fd 泄漏 + 100% CPU。**

因为对端 `close()` 之后，这个 fd 在 LT 模式下**永远处于可读状态**
（`recv` 一直返回 0），`epoll_wait` 每次都返回它，你每次都 `continue`，
于是忙等。同时 fd 不释放，`ulimit -n` 撞满后 `accept` 开始 `EMFILE`。

`test_04_epoll.sh` 阶段 [3]（fd 回收）就是专门抓这个的：

```
[3] fd 回收  并发跑完 fd 数必须回落
    fds_after <= fds_base + 2
```

## 4.2 `errno` 只在失败时被写

```cpp
int n = ::recv(fd, buf, sizeof(buf), 0);
if (n > 0) { ... }
else if (n == 0) { /* 这里读 errno 是错的！ */ }
else { if (errno == EAGAIN) ... }
```

**`recv` 返回 `0` 的时候不会写 `errno`。**
所以如果代码写成：

```cpp
if (n == 0 && errno == EAGAIN) { ... }    // ❌ 读到的是上一次调用的残留 errno
```

就会拿一个**完全无关的旧 errno** 做判断。
如果上次恰好在别的地方失败过且 `errno == EAGAIN`，这里就会误判成"没有数据，等等就好"，
于是**永远不关闭连接** → 又变成忙等。

**规则：先判断返回值，再判断 `errno`。永远不要在返回值非负时读 `errno`。**

## 4.3 业务回调里可能把连接关了

```cpp
if (n > 0) {
    if (_msg_cb) _msg_cb(fd, buf, static_cast<size_t>(n));
    {
        std::lock_guard<std::mutex> lk(_mtx);
        if (_conns.find(fd) == _conns.end()) return;   // ★ 回调里把我关了
    }
    continue;   // 继续 recv
}
```

**这是本阶段最隐蔽的 UAF。**

业务回调拿到的是裸 `int fd`，它可以做任何事——包括调用 `send`，
而 `send` 在出错时会 `del_and_close(fd)`。于是：

```
handle_read(fd)
  └─ recv 成功
  └─ _msg_cb(fd, ...)
       └─ send_wbuf(fd, ...) → 出错 → del_and_close(fd) → epoll_ctl(DEL) + _conns.erase
  └─ continue → recv(fd)         ← fd 已经被 close 了
```

`recv` 一个已关闭的 fd 返回 `-1 / EBADF`，然后 `del_and_close(fd)` 又调一次
`epoll_ctl(DEL)` → `EBADF` → 静默失败。更糟的情况是 fd 号被复用，
你在操作一条完全无关的连接。

**"回调之后必须重新验证连接还在"** 是回调式设计的铁律。
本项目的写法是对的，但代价是多了一次加锁 + hash 查找。

> 更彻底的做法：`MessageCallback` 直接传 `shared_ptr<Connection>` 而不是 `int fd`。
> 这样回调里拿到的是**强引用**，即使 `_conns` 里删了，对象也还活着，
> fd 在作用域结束前不会被 `~Connection` 关闭。
> 这是 `shared_ptr` 方案真正的价值所在——**用引用计数代替手工的"存活检查"**。

## 4.4 `int n` 应该是 `ssize_t n`

```cpp
int n = ::recv(fd, buf, sizeof(buf), 0);       // ❌ 窄化
ssize_t n = ::recv(fd, buf, sizeof(buf), 0);   // ✅
```

`recv` 返回 `ssize_t`。这里因为 `KReadBuf = 4096` 不会溢出，实际无害，
但在 32 位平台上 `int` 和 `ssize_t` 同宽、在 64 位平台上 `ssize_t` 是 64 位，
`-Wconversion` 会告警，且这是个"哪天别人把 `KReadBuf` 改成 `INT_MAX`" 就炸的定时炸弹。

**类型不匹配的 bug 从来不因为"现在不会溢出"而消失，只是延后引爆。**

## 4.5 `EAGAIN` 在读路径和写路径含义完全不同

这是最容易混淆的概念，必须在脑子里彻底分开：

| | `recv` 的 `EAGAIN` | `send` 的 `EAGAIN` |
|---|---|---|
| 内核哪个缓冲区满 | **接收**缓冲区（`tcp_rmem`） | **发送**缓冲区（`tcp_wmem`） |
| 含义 | **没有数据可读** | **没有空间可写** |
| 正确响应 | `return`，等 `EPOLLIN` | **存起来**，等 `EPOLLOUT` |
| 数据在哪 | 在对端 / 网络上 | **在你手里，没存就丢了** |

`recv` 遇 `EAGAIN` 直接返回是对的——数据还在，下次再来拿。
`send` 遇 `EAGAIN` 直接返回**是错的**——数据在你手上，一返回就永久丢了。

**这一个区别，就是整个写路径存在的理由。**

本机实测的缓冲区大小：`【实测】`

```bash
$ sysctl net.ipv4.tcp_wmem
net.ipv4.tcp_wmem = 4096    16384    4194304    # min default max
$ sysctl net.ipv4.tcp_rmem
net.ipv4.tcp_rmem = 4096    131072   6291456
```

**发送缓冲区默认只有 16 KB，接收缓冲区 128 KB。** 这个不对称很有用：
它意味着"服务器发得比客户端收得快"，所以**写路径比读路径更容易撞 `EAGAIN`**。

---

# 五、写路径：本阶段最大的坑区

这一节是本篇的核心。写路径一共 70 行代码，踩出了 8 个坑，
其中 2 个是致命的、1 个持续制造假绿。

## 5.1 正确的状态机

```
        空闲：wbuf 空 + 未注册 EPOLLOUT
          │
          │ send_wbuf(fd, data, len) → flush()
          │ send() 全部塞进内核
          ▼
        内核缓冲区满 → send() 返回 EAGAIN
          │
          │ 剩余字节 append_wbuf() + mod(fd, EPOLLIN|EPOLLOUT)
          ▼
        等待可写
          │
          │ 对端 read → TCP 窗口滑动 → 内核发送缓冲区腾出空间
          ▼
        EPOLLOUT 触发 → handle_write(fd) → flush(conn, nullptr, 0)
          │ 循环：send() + wbuf_retrieve(n) 推进
          ▼
        wbuf 排空 → mod(fd, EPOLLIN) 摘掉 EPOLLOUT
          │
          └──────────────→ 回到空闲状态
```

三点必须刻在脑子里：

1. **`wbuf` 只有在 `send` 返回 `EAGAIN` 时才可能有东西。**
   `wbuf` 非空 ⟺ 有未完成的发送 ⟺ `EPOLLOUT` 应该注册着。
   这个不变式是排查一切写路径问题的起点。

2. **`EPOLLOUT` 是 `wbuf` 非空的镜像。**
   两者必须同生同灭。任何时刻如果 `wbuf` 空了但 `EPOLLOUT` 还挂着 → 忙等；
   如果 `wbuf` 非空但 `EPOLLOUT` 没挂 → 数据永远发不出去（卡死在 `wbuf` 里）。

3. **`flush` 是唯一的发送出口。**
   不管数据来自业务（`send_wbuf`）还是来自"内核腾出空间了"（`handle_write`），
   都必须走同一个函数，否则两份实现迟早分叉。

## 5.2 坑一：`send` 成功之后忘了推进 `wbuf`（致命）

这是本阶段造成**数据丢失 76%～90%** 的根因。

错误写法：

```cpp
while (!conn_ptr->wbuf_empty()) {
    ssize_t n = ::send(fd, conn_ptr->wbuf_data(), conn_ptr->wbuf_size(), MSG_NOSIGNAL);
    if (n > 0) {
        continue;        // ❌❌❌ wbuf 一个字节都没减少，下一轮还是这些数据
    }
    ...
}
```

`wbuf` 不推进 → `while` 条件永远成立 → **同一批数据被无限重发**，
直到 `EAGAIN` 挡住为止。所以表面上"没有死循环"（`EAGAIN` 会 `return`），
但实际上客户端收到的是**重复的前缀**，后面全丢了。

正确写法：

```cpp
if (n > 0) {
    conn_ptr->wbuf_retrieve(static_cast<size_t>(n));   // ★ 消费
    continue;
}
```

### 为什么这个坑如此难发现

因为它**看起来完全正确**：
- 编译通过，零告警
- 没有死循环（`EAGAIN` 会退出）
- 没有崩溃
- 服务端日志干净
- `test_04` 六个阶段全绿

只有 strace 才能看见它。`【实测】`

```
epoll_ctl(4, EPOLL_CTL_MOD, 5, {events=EPOLLIN|EPOLLOUT, data={u32=5}}) = 0
sendto(5, "AAAA...", 5804032, MSG_NOSIGNAL, NULL, 0) =   884736
sendto(5, "AAAA...", 5804032, MSG_NOSIGNAL, NULL, 0) =  -1 EAGAIN
```

**两次 `sendto` 的请求长度都是 `5804032`。**
如果 `wbuf` 被正确消费了，第二次应该是 `5804032 - 884736 = 4919296`。

`sendto` 请求长度直方图：

```
620 × 4096          ← 正常路径（每次 recv 的数据直接 send 成功）
  1 × 5804032 → 884736
  1 × 5804032 → -1 EAGAIN    ← 长度完全没变，铁证
  1 × 74
```

**surefire 的判据：同一个缓冲区的两次 `sendto` 请求长度不变，就是没推进。**

## 5.3 坑二：`EPOLLOUT` 不摘除 = 100% CPU 忙等（致命）

修好 5.2 之后，`wbuf` 会被排空，`while` 退出。然后呢？

错误写法：

```cpp
while (!conn_ptr->wbuf_empty()) { ... }
// ★ 什么都不做就返回了
```

`wbuf` 空了，但 `EPOLLOUT` 还注册着。
LT 模式下，内核看到"发送缓冲区有空位" → **立刻返回 `EPOLLOUT`** →
`handle_write` 又被调用 → `wbuf` 是空的，`while` 一次都没进 → 立即返回 →
`epoll_wait` 又返回 → …

**典型现象**：`top` 里一个核 100%，但服务器功能完全正常，
日志不涨、内存不涨、客户端体验正常。你可能几周都不会发现。

正确写法：

```cpp
if (conn_ptr->wbuf_empty()) {
    _epoll->mod(fd, EPOLLIN);     // ★ 摘掉 EPOLLOUT
}
```

### 为什么坑一和坑二必须一起修

| 只修坑一 | 只修坑二 | 一起修 |
|---|---|---|
| 数据正确 ✅ | 数据仍然丢 ❌ | 数据正确 ✅ |
| CPU 100% ❌ | CPU 正常 ✅ | CPU 正常 ✅ |

**只修坑二甚至更糟**：`wbuf` 不推进 + `EPOLLOUT` 被摘掉 =
数据卡死在 `wbuf` 里永远发不出去，且 `EPOLLOUT` 再也不会触发。
修复前是"丢数据"，修复后变成"彻底不发了"。

这就是为什么修复清单里 ① 和 ② 是**耦合**的，必须同时落地。

### 修好之后的实测对比 `【实测】`

压测：20 连接 × 2000 消息 × 4096 字节 = 40,000 条 / 160 MB

| 指标 | `handle_write` 空壳 | 修复后 | 倍数 |
|---|---|---|---|
| `recv` | 13,302 / 40,000 | **40,000 / 40,000** | — |
| `mismatch` | 20 | **0** | — |
| `errors` | 20 | **0** | — |
| 吞吐 | 425 msg/s | **17,618 msg/s** | **41x** |
| CPU | 100% | **19.6%** | 5x ↓ |
| P50 延迟 | 30,796 ms | **1,584 ms** | **19x** |

**41 倍吞吐提升 + CPU 降 5 倍**，只改了不到 20 行。

## 5.4 坑三：新数据插到旧数据前面 = 乱序

```cpp
// ❌ 错误：wbuf 非空时直接 send 新数据
if (!conn_ptr->wbuf_empty()) {
    // 天真的想法："反正 wbuf 里有数据，直接发新的更快"
    ::send(fd, __data, __n, MSG_NOSIGNAL);
}
```

TCP 是**字节流**，顺序由内核 FIFO 保证，但如果新旧数据走不同的 `send` 调用，
顺序就由你的代码决定。

正确做法：`wbuf` 非空 ⟹ 新数据**必须**排到后面：

```cpp
if (!conn_ptr->wbuf_empty()) {
    conn_ptr->append_wbuf(data, len);   // 排队
    return;
}
```

**这个顺序在"纯冲刷"场景下尤其容易出错**，见下一个坑。

## 5.5 坑四：`flush(conn, nullptr, 0)` 什么都没做

这是本次重构中真实卡住的地方。用户的提问：

> 我的 flush 函数怎么用在 handle_write 里面，如果你把 data 设为 nullptr，
> 把 len 设为 0，那么代码里什么也没走啊

**完全正确。** 原来的 `flush` 是这个结构：

```cpp
void flush(conn_ptr, data, n){
    if (!conn_ptr->wbuf_empty()) {
        conn_ptr->append_wbuf(data, n);   // ① wbuf 非空，进这里
        return;                            // ② append(nullptr, 0) 是空操作
    }                                      // ③ 直接返回，send 循环一次都没跑
    ... // send 逻辑
}
```

`handle_write` 被调用的**前提**就是 `wbuf` 非空，所以 ① 必然成立 → ② 空操作 → ③ 返回。
**纯冲刷永远进不到 send 循环。**

### 根因：判断顺序反了

| 原顺序 | 正确顺序 |
|---|---|
| 先看有没有新数据要排队 | **先把旧欠账还干净（wbuf）** |
| 再发 | **再发新数据** |
| | **最后按结果决定 `EPOLLOUT` 的注册 / 移除** |

**只有后者，`data=nullptr, len=0` 才天然等价于"纯冲刷"**——
因为发新数据那一段 `remaining = 0`，循环体一次不进，只执行收尾逻辑。

### 正确结构

```cpp
private:

    enum class Drain { Clean, Blocked, Error };

    // 把 wbuf 里堆积的旧数据尽量推给内核
    Drain drain_wbuf(int fd, const std::shared_ptr<Connection>& conn_ptr) {
        while (!conn_ptr->wbuf_empty()) {
            ssize_t n = ::send(fd, conn_ptr->wbuf_data(), conn_ptr->wbuf_size(), MSG_NOSIGNAL);
            if (n > 0) { conn_ptr->wbuf_retrieve(static_cast<size_t>(n)); continue; }
            if (n == 0)  return Drain::Error;
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return Drain::Blocked;
            return Drain::Error;
        }
        return Drain::Clean;
    }

    // 全项目唯一的发送函数：追加与冲刷合一
    void flush(std::shared_ptr<Connection> conn_ptr, const void* data, size_t n) {
        const int fd = conn_ptr->fd();

        // ① 先还旧欠账（顺序不能反）
        switch (drain_wbuf(fd, conn_ptr)) {
            case Drain::Error:   del_and_close(fd); return;
            case Drain::Blocked:
                conn_ptr->append_wbuf(static_cast<const char*>(data), n);  // 纯冲刷时 n==0，内部守卫自动忽略
                _epoll->mod(fd, EPOLLIN | EPOLLOUT);
                return;
            case Drain::Clean:   break;
        }

        // ② 再发新数据（纯冲刷时 n==0，整段跳过）
        size_t remaining = n;
        while (remaining > 0) {
            ssize_t w = ::send(fd, static_cast<const char*>(data) + (n - remaining),
                               remaining, MSG_NOSIGNAL);
            if (w > 0) { remaining -= static_cast<size_t>(w); continue; }
            if (w == 0) { del_and_close(fd); return; }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            del_and_close(fd);
            return;
        }

        // ③ 收尾：EPOLLOUT 的生与死只在这一处决定，两种模式共用，永不遗漏
        if (remaining > 0) {
            conn_ptr->append_wbuf(static_cast<const char*>(data) + (n - remaining), remaining);
            _epoll->mod(fd, EPOLLIN | EPOLLOUT);
        } else {
            _epoll->mod(fd, EPOLLIN);      // 账清了，摘掉 EPOLLOUT
        }
    }
```

两个调用点都退化成骨架：

```cpp
void send_wbuf(int fd, const void* data, size_t n) override {
    auto conn_ptr = lookup(fd);       // 加锁查 _conns
    if (!conn_ptr) return;            // 回调里可能已经关了我
    flush(conn_ptr, data, n);         // 有新数据
}

void handle_write(int fd) {
    auto conn_ptr = lookup(fd);
    if (!conn_ptr) return;
    flush(conn_ptr, nullptr, 0);      // ★ 纯冲刷，一行
}
```

### 这个结构顺带修掉的问题

| 问题 | 修法 |
|---|---|
| `flush` 零调用点（死代码） | 两个调用点都走它 |
| `handle_write` 漏调 `wbuf_retrieve` | 统一放在 `drain_wbuf`，只有一处 |
| `_epoll->mod(fd)` 依赖默认参数 | 两处都显式传 `EPOLLIN\|EPOLLOUT` / `EPOLLIN` |
| `n == 0` 读到过期 `errno` | 单独判 `Drain::Error`，不读 errno |
| `wbuf` 排空仍挂 `EPOLLOUT` | 第 ③ 步无条件 `mod(fd, EPOLLIN)` |
| `send_wbuf` 先判 `wbuf_empty` 无法表达"纯冲刷" | 先 `drain` 再 `send`，两种语义合一 |

## 5.6 坑五：`send` 返回 0 不能继续循环

```cpp
if (n == 0) { error = true; break; }   // ★ 不是 continue
```

TCP 上 `send` 返回 0 意味着**无法推进**（不是"发完了"，`n == 0` 只在 `len == 0` 时出现）。
如果 `continue`，`remaining` 不变 → 无限循环 → 100% CPU + 永远发不出去。

**规则：`send` 的返回值 `<= 0` 一律不是"成功"。**
`> 0` 才算成功，`< 0` 看 `errno`，`== 0` 当错误。

## 5.7 坑六：`MSG_NOSIGNAL` 不能漏

```cpp
::send(fd, data, len, MSG_NOSIGNAL);
//                     ^^^^^^^^^^^^ ★
```

对端已关闭时，`send` 会产生 `SIGPIPE`。默认 `SIGPIPE` 的动作是**终止进程**。

不加 `MSG_NOSIGNAL`（或全局 `signal(SIGPIPE, SIG_IGN)`）的后果：
一个客户端在服务端发送时 `close()` → **整个服务器进程被信号杀死**。

这个 bug 的可怕之处是**它是概率性的**：连接少的时候几乎不出现，
压测的时候才偶尔来一次，看起来像"随机崩溃"。

**两种写法二选一，必须选一个：**
- 每次 `send` 带 `MSG_NOSIGNAL`（推荐，局部生效、不受全局状态影响）
- `signal(SIGPIPE, SIG_IGN)` 或 `sigaction` + `SA_RESTART`（全局，但会影响所有 fd）

本项目用了 `MSG_NOSIGNAL`，是更安全的选择。

## 5.8 坑七：`wbuf` 没有上限 = 内存炸弹

```cpp
conn_ptr->append_wbuf(data, n);      // ★ 无上限，无检查
```

如果对端**一直不读**（慢客户端、恶意客户端、或者 `nc` 连着但不 `cat`），
`wbuf` 会无限增长。

实测：64 MB 数据 + 客户端憋 8 秒不读 + `SO_RCVBUF=8192` `【实测】`

```
启动 RSS = 3,764 KB
峰值 RSS = 67,344 KB      ← 涨了 63.5 MB，基本就是 64 MB 的 wbuf
```

**一个连接就能吃掉 64 MB。** 1000 个这样的连接 = 64 GB = OOM。
而且 OOM 的时候 `new` 抛 `bad_alloc`，`std::string::append` 在极端情况下
直接 `std::terminate`（取决于编译器和标准库），**没有任何优雅降级的机会**。

这就是 `slowloris` 攻击在写路径上的等价物：**读端暂停攻击**。

修复方向（待做）：

```cpp
static constexpr size_t kMaxWbufSize = 1 << 20;   // 1 MB

if (conn_ptr->wbuf_size() > kMaxWbufSize) {
    std::cerr << "wbuf overflow, closing fd=" << fd << '\n';
    del_and_close(fd);        // 宁可断连，也不能 OOM
    return;
}
```

**产品级的选择通常不是"无限缓冲"，而是"超过阈值就断连"**——
因为一个读不动的客户端，无论如何都是要放弃的，早放弃比 OOM 好。

## 5.9 坑八：`wbuf_retrieve` 的 O(n²)

```cpp
void wbuf_retrieve(size_t len){
    if (len >= _wbuf.size()) {
        _wbuf.clear();
    } else {
        _wbuf.erase(0, len);      // ★ 每次都要 memmove 剩余全部数据
    }
}
```

`std::string::erase(0, len)` 会把后面的所有字节向前移动 `len`。
如果一次大发送被切成 N 次部分写，每次 `erase` 都要移动剩余数据：

- 总字节数 `S`，平均每次 `retrieve` 移动 `S/2`
- N 次移动总量 `O(N × S)` = **平方级**

实测场景：`wbuf` 里有 4 MB，每次 `send` 推进 16 KB（内核缓冲区大小），
需要 `4MB / 16KB = 256` 次，总移动量 `256 × 2MB ≈ 512 MB` 的内存拷贝。
**纯 CPU 浪费，且完全没有必要。**

正确做法是用**偏移量**而不是删除：

```cpp
class Connection {
    size_t _woff = 0;                    // 已发送的偏移
    std::string _wbuf;

public:
    const char* wbuf_data() const { return _wbuf.data() + _woff; }
    size_t      wbuf_size() const { return _wbuf.size() - _woff; }
    bool        wbuf_empty() const { return _woff >= _wbuf.size(); }

    void wbuf_retrieve(size_t len) {
        _woff += len;
        if (_woff >= _wbuf.size()) {     // 全部发完才真正清空
            _wbuf.clear();
            _woff = 0;
        }
    }
};
```

`retrieve` 变成 O(1)。代价是 `_wbuf` 在排空前不会释放内存
（但排空时 `clear()` 会），以及需要定期判断是否该 `shrink`。

本项目的 `_woff` 已经**声明了但没初始化、没使用**——这是个半成品，
留着比删掉更危险（看起来像"用了，但不知道哪里用"）。

## 5.10 一个容易忽略的细节：`epoll_ctl(MOD)` 的调用频率

每次 `flush` 结束都调一次 `epoll_ctl(MOD)`，即使事件集合根本没变。

在高吞吐场景下这是可观的开销：`epoll_ctl` 是一次**系统调用**，
17,618 msg/s 就是 17,618 次额外的 `syscall`。`【推算】`

优化：在 `Connection` 里存一个 `bool _epollout_on = false;`，
只在状态**翻转**时才调 `mod`：

```cpp
void set_epollout(bool on) {          // 由 EpollServer 调用
    if (_epollout_on == on) return;   // 没变就不发系统调用
    _epollout_on = on;
    _epoll->mod(fd(), on ? (EPOLLIN|EPOLLOUT) : EPOLLIN);
}
```

**这不是正确性问题，是性能问题。** 但它的存在说明了一件事：
`EPOLLOUT` 的注册状态是**连接级别的状态**，应该被显式建模，
而不是每次靠 `mod` 重新声明。

---

# 六、生命周期：double close 与空指针

这一节的坑**之前从没被记录过**，是这一轮读代码 + 实测新发现的。

## 6.1 `~EpollServer` 和 `~Epoll` 都 close 同一个 fd

```cpp
// Epoll_server.hpp
~EpollServer(){
    if (_epoll.get()->get_fd() >= 0){
        ::close(_epoll.get()->get_fd());     // ① 手动 close
    }
}

// epoll.hpp
~Epoll(){
    if(_epoll_fd != -1){
        ::close(_epoll_fd);                  // ② RAII 析构里再 close 一次
    }
}
```

`_epoll` 是 `std::unique_ptr<Epoll>`，它会在 `~EpollServer` 之后被析构。
所以同一个 fd 被 `close` 两次。

strace 铁证：`【实测】`

```
2321361 close(3)   = 0
2321361 close(4)   = 0
2321361 close(4)   = -1 EBADF (Bad file descriptor)     ← 第二次
```

（`fd 3` 是 `listen_fd`，由 `stop()` 关闭；`fd 4` 是 `epoll_fd`。）

### 为什么这很危险

在单线程、进程即将退出的场景下，`EBADF` 无害——反正要死了。

但**在下面这些情况下是真正的 double free**：

1. `close(4)` 之后、第二次 `close(4)` 之前，另一个线程 `accept` 出一个新连接，
   恰好也拿到 `fd 4`。于是第二次 `close` 关掉的是**新连接**。
2. 用户在 `~EpollServer` 里 `close` 后，又把 `epoll_fd` 拿去做了别的事
   （比如作为 `eventfd`），第二次 `close` 关掉的是那个对象。

**这是教科书级的 double free，只是当前恰好踩在"进程要退出"的安全窗口里。**

### 修法

**`close` 只能有一个 owner。** 既然 `Epoll` 已经是 RAII 类、析构里已经关，
`~EpollServer` 就**什么都不该做**：

```cpp
~EpollServer(){
    // _epoll 是 unique_ptr，~Epoll 里已经 close 了。
    // 这里千万不要再 close 一次。
}
```

或者直接删掉整个析构函数。

## 6.2 `run_loop` 没跑过时，`~EpollServer` 解引用空指针

```cpp
~EpollServer(){
    if (_epoll.get()->get_fd() >= 0){    // ★ _epoll 可能是 nullptr
```

`_epoll` 只在 `epoll_init()` 里被赋值：

```cpp
_epoll = std::make_unique<Epoll>(EPOLL_CLOEXEC, 64);
```

而 `epoll_init()` 只从 `run_loop()` 里调用。所以如果对象构造后
**还没调用 `start()` 就被销毁**——比如配置解析失败、端口冲突、
单测里只想验证构造、或者 `main` 里提前 `return`——`_epoll` 就是 `nullptr`，
`_epoll.get()->get_fd()` 直接解引用空指针。

实测：`【实测】`

```cpp
int main(){ EpollServer s(6398); return 0; }   // 构造后直接析构，start() 未调用
```

```
Segmentation fault (core dumped)
exit=139
```

**"析构函数必须能在对象生命周期的任何阶段被安全调用"** 是 RAII 的基本要求。
一个可能为空的 `unique_ptr` 必须先判空：

```cpp
~EpollServer(){
    // 最安全的做法是什么都不做（见 6.1）
}
```

如果确实要做事：

```cpp
if (_epoll && _epoll->get_fd() >= 0) { ... }
```

## 6.3 `del_and_close` 的顺序：先 `epoll.del` 再 `erase`

```cpp
void del_and_close(int fd){
    _epoll.get()->del(fd);              // ① 先从 epoll 摘掉
    std::lock_guard<std::mutex> lk(_mtx);
    _conns.erase(fd);                   // ② 再删 map → 引用计数归零 → ~Connection → close
}
```

**顺序反了会静默失效。**

如果先 `erase`：`~Connection` 触发 `close(fd)`，fd 被内核回收。
然后 `epoll_ctl(EPOLL_CTL_DEL, fd)` —— 此时 `fd` 已经关闭，返回 `EBADF`，
**`epoll_ctl` 失败，但这个监视条目仍在 epoll 内部，只是对应的 fd 号已经空出来了**。

更糟：如果在这两步之间 `accept` 拿到同一个 fd 号，
`epoll_ctl(DEL)` 会**成功**——但它删掉的是**新连接**的监视。
新连接从此收不到任何事件，静默失效。

**这就是 `note_stage1~5` 里反复出现的"顺序错误导致静默失效"的 epoll 版本。**

## 6.4 `shared_ptr<Connection>` 真正的价值

```cpp
std::shared_ptr<Connection> conn_ptr;
{
    std::lock_guard<std::mutex> lk(_mtx);
    auto it = _conns.find(fd);
    if (it == _conns.end()) return;
    conn_ptr = it->second;              // ★ 引用计数 +1
}
// 从这里开始，即使别的线程 del_and_close(fd)，
// conn_ptr 也保证对象（以及 fd）活着直到本函数返回
```

`close` 只出现在 `~Connection` 里。所以：

- `_conns.erase(fd)` → 引用计数 -1
- 如果还有人持有 `conn_ptr` → 计数不为 0 → **不会析构 → 不会 close**
- 最后一份引用消失 → 析构 → `close(fd)`

实测验证（临时程序）：`【实测】`

```
fd=3  ref=1
拷贝后 ref=2
离开作用域后 ref=1
```

**这个机制让"fd 在被使用时被关闭"这类 bug 从根本上消失。**
代价是每次访问连接表都要加锁 + 一次 `shared_ptr` 拷贝（原子递增）。

之前讨论过的问题："为什么要先关闭 fd 再 append，conn_ptr 不会因为 fd 被关了然后也死掉吗"
——答案是**不会**。`conn_ptr` 是强引用，fd 的关闭时机由最后一个 `shared_ptr` 决定，
而不是由 `_conns.erase` 决定。这正是 RAII + 引用计数组合的价值。

## 6.5 `_listen_fd` 与 `stop()` 的双重 close（已知未修）

```cpp
void stop(){
    _running = false;
    if (_idle_fd >= 0) { ::close(_idle_fd); _idle_fd = -1; }
    if (_listen_fd >= 0) { close(_listen_fd); _listen_fd = -1; }
}
```

`stop()` 可能被信号处理器和析构函数**两个线程/上下文**同时调用。
`_listen_fd` 不是原子的，两次 `close` 之间存在窗口。

代码里已经有注释承认了这一点：

> 两个线程交错执行时可能 close 两次同一个 fd，
> 而第二次 close 时这个号可能已经被别的线程复用成客户端连接了 —— 那是灾难性的。

修法：`_listen_fd` 改成 `std::atomic<int>`，用 `exchange(-1)` 保证只有一个赢家：

```cpp
int fd = _listen_fd.exchange(-1);
if (fd >= 0) ::close(fd);
```

---

# 七、调试方法论：怎么把"看起来对"证伪

这一节的结论比任何代码都重要。

## 7.1 判断标准不是"测试过了"

整个写路径的调试过程体现了一个原则：

> **判断标准不是"测试过了"，是"我让它失败过，然后它才过的"。**

`handle_write` 空壳的版本通过了 `test_04` 全部六个阶段。
如果只信测试，这个 bug 会一直存在。

**能通过测试的代码只是"没被测试抓到"，不等于"正确"。**

## 7.2 strace：看见内核眼里的真相

这是本阶段最有用的工具。命令：

```bash
strace -f -p $SRV -e trace=sendto,close,epoll_ctl -o /tmp/tr.log
```

**为什么是这几个系统调用**：

| 系统调用 | 能看到什么 |
|---|---|
| `sendto` | 每次发送的**请求长度**和**实际写入长度** |
| `epoll_ctl` | `EPOLLOUT` 什么时候注册、什么时候摘除 |
| `close` | 是否 double close、什么时候关 |

**核心技巧：看请求长度有没有变。**
"请求 5804032，写了 884736"，下一次还是"请求 5804032" → 缓冲没推进。

还可以数 `sendto` 的请求长度分布，一眼看出代码走了哪条路径：

```bash
strace -e trace=sendto -o /tmp/s.log ...
grep -o 'sendto([0-9]*, "[^"]*", [0-9]*' /tmp/s.log | awk -F', ' '{print $NF}' | sort -n | uniq -c | sort -rn
```

输出：

```
620 × 4096          ← 正常路径
  1 × 5804032       ← wbuf 路径
  1 × 74
```

**如果只看到 4096，说明 `wbuf` 路径一次都没走过——这本身就是个重要信息。**

## 7.3 强制触发 `EAGAIN` 的配方

这是本阶段花时间最多的地方。**默认情况下你几乎不可能撞到 `EAGAIN`。**

失败的尝试：

| 尝试 | 结果 |
|---|---|
| 200 KB 单连接 | 49 次 `send`，**0 次 `EAGAIN`**（loopback 缓冲区吃掉了） |
| 8 MB 但客户端并发读 | 1976 次 `send`，**0 次 `EAGAIN`**，总数精确 8,000,000 |
| 1 MB 发完睡 6 秒 | 客户端收到精确 1,000,000 字节，CPU 0% |

**原因是默认发送缓冲区 16 KB，但 loopback 上接收端会持续消费，
TCP 窗口自动滑动，发送缓冲区永远有新空间。**

最终有效的配方（三个条件缺一不可）：

```python
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)  # ① 压小接收窗口
sender.start()                                              # ② 发 ≥ 8 MB
time.sleep(6)                                               # ③ 憋住不读
```

三个条件的物理意义：

1. **`SO_RCVBUF = 8192`** → 缩小 TCP 接收窗口 → 对端（服务端）的
   发送缓冲区无法被对端确认释放 → 服务端 `send` 迟早撞 `EAGAIN`
2. **数据量 ≥ 8 MB** → 足够填满"服务端发送缓冲区 + 客户端接收缓冲区 + 在途数据"
   的总和（16 KB + 2×8 KB + ...，实测需要 MB 级）
3. **憋住不读** → 窗口不再滑动，缓冲区不会自己腾出来

**没有第 3 条，前两条都没用**——客户端一读，窗口就滑，`EAGAIN` 就不来了。

## 7.4 `/proc` 采样：量化"增长"而不是"有没有"

排查内存问题时，看一次 `VmRSS` 没有意义（快照）。要看**峰值**：

```bash
PID=$1
max=0
for i in $(seq 1 160); do
  r=$(awk '/VmRSS/{print $2}' /proc/$PID/status)
  [ "$r" -gt "$max" ] && max=$r
  sleep 0.1
done
echo "PEAK_RSS=${max}KB"
```

同一次 64 MB 测试：

| 采样方式 | 结果 |
|---|---|
| 测试结束后看一次 | 3,856 KB（**看起来没问题**） |
| 全程 0.1s 采样取峰值 | **67,344 KB**（wbuf 撑到 64 MB） |

**"测试结束后看"是典型的错误观测方式**——那时候 `wbuf` 已经排空、内存已经释放。
必须持续采样。

## 7.5 gdb 验证 `O_NONBLOCK`

不用改代码、不用加日志，直接在调试器里问内核：

```gdb
(gdb) p (int)fcntl(this->_listen_fd, 3, 0)     # F_GETFL = 3
$1 = 2050                                       # 0x802 = O_RDWR | O_NONBLOCK
```

`O_NONBLOCK` 在 Linux 上是 `0x800` = 2048，`O_RDWR` 是 `0x2`。
`2050 = 0x802` → 两者都有 → 非阻塞已生效。

**`0x2` 说明只是"非阻塞没设置"，`0x0` 说明 fd 本身有问题。**
这个技巧适用于任何"我改了 `fcntl` 但不确定生效没有"的场景。

## 7.6 三个脚本的分工

本阶段最后沉淀了三个互补的验证工具，**缺一个就会漏一类 bug**：

| 脚本 | 客户端行为 | 抓什么 |
|---|---|---|
| `test_04_epoll.sh` [6] | 正常收发，64 B 小包 | 粘包/基础正确性（**撞不到 `EAGAIN`**） |
| `bigfile_echo.py` | 正常收发，MB 级大包 | "正常情况不丢" |
| `manual_check_wbuf.py` | **`SO_RCVBUF=8192` + 憋 6 秒** | **"极端情况不重复发送"** |

`manual_check_wbuf.py` 的判据是**放大倍数**：

```
放大倍数 = 客户端收到字节 / 客户端发送字节

1.00x  → ✅ 收发平衡
>1.05x → 🔴 wbuf 被重复发送（wbuf_retrieve 没调 / 参数错）
<0.95x → 🔴 数据丢失
```

修复前的实测：`【实测】`

| 数据量 | 客户端发送 | 客户端收到 | 放大倍数 | 结论 |
|---|---|---|---|---|
| 8 MB | 8,388,608 | 1,982,026 | **0.24x** | 丢 76.4% |
| 20 MB | 20,971,520 | 1,969,152 | **0.09x** | 丢 90.6% |

**这两个数字是我第一次真正看见"丢数据"的形态**——
之前所有测试都是绿的，只有这个脚本把它变成了可复现的数字。

## 7.7 一个必须知道的脚本自身缺陷

`manual_check_wbuf.py` 内部有 `deadline = time.time() + 25` 和 `settimeout(3)`。

但 `SO_RCVBUF = 8192` 时，内核把实际值放大到 16 KB，
每轮 `recv` 只能拿回约 14.7 KB。收完 8 MB 需要：

```
8,388,608 / 14,720 ≈ 570 轮 × 每次 recv 的往返延迟
```

实测完整收完耗时 **112 秒**，远超 25 秒的 `deadline`。

于是脚本在 25 秒时只收到约 1.9 MB，**误报"缺失 76%"**。

**这个误报和真实的"丢 76%" 数值上几乎一样**，非常容易混淆。
判读必须以输出末尾的**最终累计值**为准：

```
t=112.68 收到 9856，累计 8388608
t=112.68 全部收到            ← 这才是真相
```

**教训：一个测试工具本身可能是错的。看到"失败"的第一反应应该是
"这个判据可信吗"，而不是"代码有 bug"。**

---

# 八、假绿识别手册

本阶段最大的收获是学会了识别"假绿"。整理成清单：

## 8.1 类型一：测试数据量不够，路径根本没走到

| 假绿 | 真相 |
|---|---|
| `test_04` [6] 64 B × 500 全绿 | 总数据量 32 KB < 16 KB×2，`EAGAIN` 一次都没触发 |
| 200 KB 单连接 strace 0 次 EAGAIN | loopback 缓冲区足够大 |
| 8 MB 但客户端边发边读 | 窗口持续滑动，永远不饱和 |

**判据：如果代码里有一条 `EAGAIN` 分支，你必须在日志里**亲眼看见**它被执行过。**
没看见 = 没测过 = 未知。

## 8.2 类型二：断言只查"没崩"，不查"数据对"

| 弱断言 | 强断言 |
|---|---|
| 客户端连上了 | 客户端收到的字节数 == 发送的字节数 |
| 服务端还活着 | `mismatch == 0` 且 `放大倍数 == 1.00x` |
| 没有报错日志 | strace 里 `sendto` 的请求长度在递减 |

**"没崩溃"是最弱的正确性。** 数据丢失、重复发送、乱序，全都不崩。

## 8.3 类型三：只在安全窗口里观测

| 观测时机 | 问题 |
|---|---|
| 测试**结束后**看 RSS | `wbuf` 已排空，看不到峰值 |
| 只在**空闲时**看 CPU | LT 忙等只在有连接时出现 |
| 只在**连接少**时测 | `SIGPIPE`、fd 复用、`backlog` 溢出全都不出现 |

**必须在"最坏时刻"观测，而不是"方便的时刻"。**

## 8.4 类型四：程序的默认值不是你以为的那个

```
$ ./build/bench_server --port=6200        # 不带 --server
```

默认 `model = "thread"`——**你测的是线程模型，不是 epoll**。`【实测】`

同理 `--server=Epoll` 会返回 exit 2（代码里是全小写 `epoll`）。

**测试环境和你以为的测试环境不一致，是"假绿"最常见的来源。**
每次开测前先确认：

```bash
ps -ef | grep bench_server          # 到底跑的是哪个模型
cat /proc/$PID/cmdline | tr '\0' ' '
```

## 8.5 类型五：测试脚本自己有 bug

`manual_check_wbuf.py` 的 25 秒 deadline 就是例子（见 7.7）。

**第一次看到"失败"时，先怀疑工具，再怀疑代码。**
方法：把工具指向一个已知正确的实现，看它是否仍然报错。
如果报错 → 工具错了。

---

# 九、坑位总表

按"踩到的顺序"排列，共 34 条。

## epoll 三原语（8 条）

| # | 坑 | 后果 | 严重度 |
|---|---|---|---|
| 1 | `epoll_create1` 不带 `EPOLL_CLOEXEC` | fd 泄漏到子进程 | 🟡 |
| 2 | `epoll_event.data` 不设置 | `wait` 返回垃圾 fd | 🔴 |
| 3 | 只 `reserve` 不 `resize` 就传 `data()` | 堆越界写 | 🔴 |
| 4 | `maxevents` > 缓冲区容量 | 内核越界写 | 🔴 |
| 5 | 遍历整个 `event_poll` 而非前 `n` 个 | 重复处理旧事件 | 🔴 |
| 6 | `epoll_wait` 的 `EINTR` 不重试 | 一个信号就退出服务器 | 🟠 |
| 7 | `EPOLLIN\|\|EPOLLOUT`（逻辑或）| `EPOLLOUT` 永不触发，零告警 | 🔴 |
| 8 | 移动构造不带 `noexcept` | `vector<Epoll>` 编译失败 | 🟡 |

## listen fd 与 accept（5 条）

| # | 坑 | 后果 | 严重度 |
|---|---|---|---|
| 9 | `listen_fd` 未设 `O_NONBLOCK` | drain 循环永久阻塞，reactor 静默死锁 | 🔴 |
| 10 | `accept4` 不带 `SOCK_NONBLOCK` | 新连接是阻塞的，reactor 又卡死 | 🔴 |
| 11 | `backlog = 5` | 高并发建连时连接被丢弃 | 🟡 |
| 12 | `EMFILE` 时 `continue` | 100% CPU + 每秒几十万行日志 | 🔴 |
| 13 | `EAGAIN` 与 `EINTR` 混为一谈 | 日志刷爆 / 该重试的不重试 | 🟠 |

## 事件分发（5 条）

| # | 坑 | 后果 | 严重度 |
|---|---|---|---|
| 14 | `fd == _listen_fd` 不先判断 | `listen_fd` 被 `del_and_close`，服务器不再接受连接 | 🔴 |
| 15 | `EPOLLERR` 不优先处理 | LT 模式永久返回 → 100% CPU | 🔴 |
| 16 | `EPOLLHUP` 时直接关闭不读 | 丢失对端最后一批数据 | 🟠 |
| 17 | `EPOLLIN` / `EPOLLOUT` 写成 `else if` | 写事件被读事件饿死，`wbuf` 无限增长 | 🟠 |
| 18 | 同轮内 `fd` 复用（改造 `IoThreadPool` 后） | 给新连接发旧连接的数据 | 🟠 |

## 读路径（5 条）

| # | 坑 | 后果 | 严重度 |
|---|---|---|---|
| 19 | `recv == 0` 不关闭连接 | fd 泄漏 + 100% CPU 忙等 | 🔴 |
| 20 | 返回值非负时读 `errno` | 拿残留 errno 误判，永不断连 | 🔴 |
| 21 | 回调后不复查 `_conns` | **UAF**：操作已关闭甚至已复用的 fd | 🔴 |
| 22 | `int n` 接 `recv` 返回值 | 窄化，`-Wconversion` 告警 | ⚪ |
| 23 | 混淆 `recv` / `send` 的 `EAGAIN` 语义 | 该存的没存，数据永久丢失 | 🔴 |

## 写路径（9 条）

| # | 坑 | 后果 | 严重度 |
|---|---|---|---|
| 24 | `send` 成功后不 `wbuf_retrieve` | **同一批数据无限重发，丢 76%~90%** | 🔴🔴 |
| 25 | `wbuf` 排空不摘 `EPOLLOUT` | **LT 忙等，100% CPU** | 🔴🔴 |
| 26 | 新数据插到 `wbuf` 旧数据前面 | 字节流乱序 | 🔴 |
| 27 | `flush(nullptr, 0)` 因判断顺序反了而空转 | 纯冲刷完全失效 | 🔴 |
| 28 | `send == 0` 时 `continue` | 死循环 + 永远发不出去 | 🔴 |
| 29 | 漏 `MSG_NOSIGNAL` | 对端先关 → `SIGPIPE` 杀死整个进程（概率性） | 🔴 |
| 30 | `wbuf` 无上限 | 单连接 64 MB，慢客户端打爆内存 | 🟠 |
| 31 | `wbuf_retrieve` 用 `erase(0,len)` | O(n²) 内存拷贝 | 🟡 |
| 32 | 每次都 `epoll_ctl(MOD)` | 多余的 syscall，1.7 万次/秒 | 🟡 |

## 生命周期（3 条）

| # | 坑 | 后果 | 严重度 |
|---|---|---|---|
| 33 | `~EpollServer` 和 `~Epoll` 双重 `close` | `EBADF`；跨线程场景是**真 double free** | 🟠 |
| 34 | `_epoll` 为空时析构解引用 | **段错误**（构造后不 `start()` 就退出） | 🟠 |

另外两条与 epoll 无关但同阶段发现的老坑：

| # | 坑 | 后果 |
|---|---|---|
| 35 | `del_and_close` 里 `erase` 写在 `epoll.del` 之前 | 删掉**新连接**的监视，静默失效 |
| 36 | `_listen_fd` 非原子 + `stop()` 可能被双重调用 | fd 被复用后误关别人的连接 |

---

# 附：一页速查

## 写路径的核心不变式

```
wbuf 非空  ⟺  有未完成的发送  ⟺  EPOLLOUT 已注册
```

**任何时刻这条不成立，就是 bug。** 两个方向都会出问题：

- `wbuf` 空但 `EPOLLOUT` 挂着 → 100% CPU 忙等
- `wbuf` 非空但 `EPOLLOUT` 没挂 → 数据永远卡在 `wbuf` 里

## 三分钟定位法

| 症状 | 第一嫌疑 | 验证命令 |
|---|---|---|
| 进程在、CPU 0%、无响应 | 某个阻塞调用（`listen_fd` / 新 fd 没设非阻塞） | `cat /proc/PID/stack`（内核态栈） |
| 进程在、CPU 100% | `EPOLLOUT` 未摘 / `EPOLLERR` 未处理 / `EMFILE` 未 `break` | `strace -c -p PID` 看哪个 syscall 在刷 |
| 进程消失、退出码非 0 | `SIGPIPE` / `SIGSEGV` | `dmesg \| tail`、`ulimit -c unlimited` + gdb |
| 数据丢失 / 重复 | `wbuf` 未推进 / 顺序错 | `strace -e sendto` 看请求长度是否递减 |
| 内存持续增长 | `wbuf` 无上限 | 0.1s 采样 `/proc/PID/status` 取峰值 |

## 一句话总结

> **epoll 的正确性不体现在"能跑"，体现在"每个 EAGAIN 分支都被执行过"。**
> 没亲眼看见那条分支跑，就等于没写。