# 阶段1：TCP 服务端骨架 —— 踩坑笔记

对应流程：`socket() -> bind() -> listen() -> accept() -> recv() -> close()`

***

## 坑 1：setsockopt 端口复用——踩了两次的参数问题

**症状**：明明写了复用，还是 `bind failed: Address already in use`。

**第一次错**：把值当指针传

```cpp
setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char*) bReuseaddr, sizeof(bool));
```

`(const char*)true` 是把值 1 强转成指针，等于让内核去读**地址 0x1** → 失败（EFAULT）。
而且返回值存进 `opt` 后从没检查过，失败了也不知道。

**第二次错**：地址传对了，但类型/长度不对

```cpp
setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &bReuseaddr, sizeof(bool));  // sizeof(bool)==1
// → Invalid argument (EINVAL)
```

内核对 SO\_REUSEADDR 这类开关选项要求 `optlen >= sizeof(int)`（4 字节），bool 只有 1 字节。

**正确写法**：

```cpp
int optval = 1;
int opt = setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
if (opt < 0) perror("setsockopt");   // 凡返回值，必须检查
```

**结论**：

- `option_value` 参数要的是"选项值的**地址**"，不是值本身
- 系统接口的类型照 man page 来，用 `int` 就别用 `bool`
- 返回值不检查 = 把失败藏起来

***

## 坑 2：SO\_REUSEADDR 不是万能的

它只解决 **TIME\_WAIT 残留**（连接关闭后端口冷却 60 秒）导致的 bind 失败。

如果端口上有进程**正在 LISTEN**，加了它照样 `Address already in use`——那需要 `SO_REUSEPORT`。

**排查命令**：

```bash
ss -tlnp sport = :3006    # 有没有人监听（-l = LISTEN 状态，-p = 显示进程 pid）
ss -tn  sport = :3006     # 有没有残留连接（无 -l 就看不到监听）
```

结果三种：

- 有 `LISTEN` 行 → 有活进程，先杀
- 有 `TIME_WAIT` → 内核残留，等 60 秒或靠 SO\_REUSEADDR
- 什么都没有 → 端口干净

***

## 坑 3：杀不掉的进程——Ctrl+Z 之殇（踩了两次）

**症状**：`kill <pid>` 没反应，进程还占着端口。

**原因**：跑服务端时按了 **Ctrl+Z**（暂停）而不是 **Ctrl+C**（中断）。
进程状态变成 `T`（Stopped），一直睡着，SIGTERM 送进去没人处理，自然"杀不掉"。

**解法**：

```bash
kill -9 <pid>              # SIGKILL，暂停中的进程也能杀
# 或按名字全清
pkill -9 -f socket_server; pkill -9 strace
```

**两个按键的区别**：

| 按键     | 信号      | 后果             |
| ------ | ------- | -------------- |
| Ctrl+C | SIGINT  | 进程退出，连接正常关闭    |
| Ctrl+Z | SIGTSTP | 进程挂起，fd 和端口全占着 |

**口诀**：服务端一律用 Ctrl+C 停；不小心 Ctrl+Z 了用 `fg` 唤回再 Ctrl+C，
或者 `jobs` 查、`fg` 唤回、`bg` 放后台、`kill -9` 处决。

***

## 坑 4：accept 的参数传错

**原型**：`int accept(int fd, struct sockaddr *addr, socklen_t *addrlen)`

1. **第三个参数必须传指针**（`socklen_t*`），不是传 `sizeof(...)` 这个值。
   它是"传入缓冲区大小、回填实际长度"的双向参数：
   ```cpp
   socklen_t len = sizeof(client_addr);
   int fd = accept(listen_fd, (struct sockaddr*)&client_addr, &len);
   ```
2. **第二个参数是接收客户端地址的输出参数**。别复用 `server_addr`——
   会把 bind 时填好的服务器地址覆盖掉，应另声明一个 `sockaddr_in` 接收。
3. **第二个参数需要强转** **`(struct sockaddr*)`**：`sockaddr_in` 是具体类型，
   `sockaddr*` 是通用类型，小转大要显式告诉编译器；`&len` 类型天然匹配不用转。

***

## 坑 5：打印 recv 缓冲区——字节流没有 '\0'

**症状**：`cout << buf` 可能把栈上的垃圾内存全打出来，甚至越界崩溃。

**原因**：`recv` 收到的字节流**不保证以** **`\0`** **结尾**，而 `<< char*` 会一直打到 `\0`。

**正确姿势**：永远按返回值 `n` 来：

```cpp
std::cout.write(buf, n);            // 或 std::string(buf, n)
```

**三个返回值的含义**：

| 返回值    | 含义                              |
| ------ | ------------------------------- |
| `> 0`  | 实际收到的字节数（下次从 buf 头重新用即可）        |
| `== 0` | 对端正常关闭（FIN/EOF），recv 立刻再调还会返回 0 |
| `< 0`  | 出错，errno 看细节（如 ECONNRESET）      |

这是"TCP 是字节流、没有消息边界"的第一次露脸——阶段 4 粘包拆包的老根。

***

## 坑 6：循环退出条件想反了

- `n > 0` = 刚成功收到数据 → **继续**的条件，不是退出条件
- `n == 0` = 对端关闭 → **唯一正确的常规退出点**
- `n < 0` → 出错，也要 break，否则死循环刷错误

```cpp
while (true) {
    char buf[1024] = {0};
    ssize_t n = recv(accept_res, buf, sizeof(buf), 0);
    if (n > 0) { std::cout.write(buf, n); std::cout << std::endl; continue; }
    else if (n == 0) { std::cout << "client close" << std::endl; }
    else { perror("recv error"); }
    break;   // 0 和 <0 都要出去（漏了它 → 疯狂刷屏）
}
```

***

## 坑 7：缓冲区开 10M 直接段错误

局部数组在**栈**上，Linux 默认栈只有 **8MB**（`ulimit -s` 查看）。
`char buf[10*1024*1024]` 一跑就崩。习惯值：`char buf[1024]`；
真要大缓冲区得放**堆**上（`new`/`malloc`，阶段 2 再展开）。

***

## 坑 8：nc 的 Ctrl+D 没反应

Ctrl+D 的本义只是"标准输入 EOF"，**默认不关连接**——OpenBSD 版 nc 收到 EOF
后连接还留着，服务端依然卡在 recv。

```bash
nc -N 127.0.0.1 3006     # EOF 后立刻 shutdown 写方向（发 FIN）
nc -q 0 127.0.0.1 3006   # 传统版 netcat 的等价参数
```

**Ctrl+D 和 Ctrl+C 结果一样（都是 recv == 0）**：因为 Ctrl+C 让 nc 正常退出，
内核照样替它发 FIN——错误的 `recv < 0`（RST）需要"连接关了之后还有数据硬塞进来"
（比如服务端先退出、客户端还在发第二行 → `Connection reset by peer`）。

***

## 实验记录（strace 看系统调用）

```bash
strace -f -e trace=network ./build/socket_server 2>&1 | head -40
```

1. `bind` 里看到的端口不是十进制 3006——`htons` 转成网络字节序（大端）后的值，
   机器是小端所以字节是反的。不转则内核读到完全错误的端口。
2. strace 停在 `accept(...)` 不动 = **进程睡在内核里等连接**。
   队列空就把进程摘下睡眠，nc 一连上网卡中断唤醒它、accept 返回新 fd。
   紧接着 `recv(...)` 又停住——同一个把戏。**阻塞 I/O 的本质就在这一行**。
3. `memset`、`std::cout`（大部分情况）不是系统调用——用户态自己能干完的不进内核；
   动网卡/磁盘/进程的才进（socket/bind/listen/accept/recv/close）。

***

## 遗留问题（后续课程）

- [ ] `listen(fd, 5)` 的 5 是什么队列的长度？（半连接队列 / 全连接队列）
- [ ] 字节流没有边界 → 阶段 4 用什么协议解决？（长度头）
- [ ] 服务端一次只服务一个客户端 → 阶段 2/3 的并发模型怎么破？

