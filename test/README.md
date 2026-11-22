# test/ 目录说明

测试按**「测什么」**分目录，不按文件类型分。

```
test/
├── unit/       纯内存单元测试（C++，无网络，无 socket）
├── tlv/        TLV 协议参考实现（Python 库 + 自测）
├── suites/     端到端测试套件（启动服务端 + 客户端 + 断言）
├── net/        压测/工具脚本 + bench 专用服务端入口 + 数据文件
├── debug/      gdb / strace 调试脚本
└── archive/    历史遗留、已被替代，保留备查
```

## 各目录职责

### `unit/` —— 单元测试，不碰网络

| 文件 | 被测对象 | 怎么跑 |
|---|---|---|
| `blocking_queue_test.cpp` | `lib/blocking_queue.hpp` | `test/suites/02_blocking_queue.sh` |
| `frame_test.cpp` | `lib/frame.hpp`（**96 断言**） | 手动（见下） |
| `thread_pool_test.cpp` | `lib/thread_pool_v2.hpp` | 手动（见下） |

**判据：不 import socket，不 fork 进程，毫秒级返回。** 这类测试改一行代码就能跑，
是开发时的高频反馈来源。

`frame_test.cpp` 的运行方式：

```bash
g++ -std=c++17 -Wall -Wextra -Werror -Ilib test/unit/frame_test.cpp \
    -o /tmp/frame_test && /tmp/frame_test
# ----- pass=96  fail=0 ----- OK
```

它覆盖 11 组：字节布局（含大端落地校验）/ 空 body / 二进制 body /
roundtrip（emoji + 极端 `seq`/`flag`）/ 粘包两帧 / 逐字节喂 / 头部分裂 /
坏 magic → `BAD_ERROR` / **坏 checksum → `ERROR` 且后一帧仍可解** /
超长 `len` / `seq`/`flag` 默认值与边界值。

> 写这类测试的一条硬规矩：**长度断言一律写成 `kHeaderSize + body.size()`，
> 禁止出现 `== 24` 这类魔法数字**。否则头长一改，整个测试文件全废。

### `tlv/` —— 协议层的 Python 参考实现

`tlv_probe.py` 既是**库**也是**自测**：

```bash
python3 test/tlv/tlv_probe.py        # 8 组纯内存自测，不需要服务端
```

它的存在意义是**交叉验证**：C++ 的 `frame.hpp` 和 Python 的 `pack()`
必须产出完全相同的字节。任何一边偷改了偏移/字节序/校验范围，对照立刻失败。

作为库被 `suites/05_tlv.sh` 和其他脚本 import。

### `net/` 里的探针脚本

| 文件 | 作用 | 被谁用 |
|---|---|---|
| `bench_client.py` | 压测客户端，`summary()` 输出 p50/p90/吞吐 | `03` `04` |
| `bench_server.cpp` | bench 专用服务端入口，参数化选择模型 | `03` `04` `05` `06` |
| `idle_probe.py` | **空闲超时探针**，三模式见下 | `06_timer.sh` |

`idle_probe.py` 的三种模式对应三种断言方向：

| 模式 | 做什么 | 断言什么 | 抓什么缺陷 |
|---|---|---|---|
| `die` | 连上，一个字节都不发 | 服务端应在 `idle*3+1` 秒内断开 | 超时根本没生效 |
| `alive` | 每 `--interval` 发一次 echo，持续 `--duration` | 一次都不能断 | 刷新点漏了 send 成功 |
| `starve` | pool 专用：占满 worker 后再试正常请求 | 超时前饿死、超时后恢复 | 用了 `close` 而非 `shutdown` |

**`die` 模式的两个细节**（否则会把正常行为判成失败）：

- 服务端断开时客户端可能看到 **FIN**（`recv` 返回 `b""`）
  或 **RST**（`ConnectionResetError`）——两种都算正常。
  只接受 FIN 是这类测试最常见的假红来源。
- 必须在**尚未实现**超时的服务端上先跑一次，确认它如实报
  `close_kind=still_alive`。探针本身假绿比被测代码有 bug 更危险。

```bash
# 手工验证探针（不需要跑整个 suite）
python3 test/net/idle_probe.py --port=6500 --mode=die --idle-timeout=2 --json
```

### `suites/` —— 端到端测试套件

统一约定：`set -uo pipefail` + `TMPDIR` + `trap cleanup` +
`退出码 0=PASS 1=FAIL 2=环境问题`。

| 脚本 | 测什么 | 依赖 |
|---|---|---|
| `01_emfile.sh` | `accept()` 遇到 EMFILE 时会不会自旋烧 CPU | `socket_server` |
| `02_blocking_queue.sh` | `BlockingQueue` 功能 + `-O2` + TSan 三档 | `unit/blocking_queue_test.cpp` |
| `03_pool_starvation.sh` | 线程池 worker 被占满后的饿死 + 背压 | `bench_server` `bench_client.py` |
| `04_epoll.sh` | epoll 模型七阶段（功能冒烟 / 并发冒烟 / fd 回收 / 空闲连接 / 线程解耦 / 流水吞吐 / 大包压力） | `bench_server` `bench_client.py` |
| `05_tlv.sh` | **TLV 协议链路**：往返 / seq 回填 / flag / 粘包 / 拆包 / 坏帧 | `bench_server` `tlv_probe.py` |
| `06_timer.sh` | **空闲超时**：该杀的杀掉 / 不该杀的留住 / 混合场景 / 阈值边界 | `bench_server` `idle_probe.py` |

```bash
./test/suites/05_tlv.sh                     # 默认 pool 模型
./test/suites/05_tlv.sh --server=epoll      # 换模型
./test/suites/05_tlv.sh --strict            # 坏帧不断连也判 FAIL

./test/suites/06_timer.sh                   # 默认 epoll + 2s 超时
./test/suites/06_timer.sh --server=reactor  # 主从 reactor 落地后直接套用
```

### 定时器的测试范围为什么只有 epoll

| 模型 | 定位 | 做定时器吗 |
|---|---|---|
| `thread`（one-per-conn） | **基准实现**，用来对照验证别的东西 | ❌ 不做 |
| `pool` | 第三种服务器到主从 reactor 之间的**过渡板** | ❌ 不做 |
| `epoll` | 目标形态的单线程版 | ✅ **做** |
| 主从 reactor（第四种） | 目标形态 | ✅ **做**，复用 epoll 的同一套 |

理由：`thread` 和 `pool` 的 client fd 来自阻塞的 `accept()`
（`src/server/tcp_server.cpp:57`），要踢掉一条死连接得先把阻塞在 `recv`
上的 worker 唤醒 —— 也就是必须走 `shutdown(SHUT_RDWR)` 而不能 `close`。
而 `epoll` 用的是 `accept4(..., SOCK_NONBLOCK)`（`Epoll_server.hpp:188`），
fd 本来就是非阻塞的，`del_and_close` 就是全部动作，没有这层麻烦。

主从 reactor 落地后，`pool` 的能力会被吸收进 Sub Reactor 的 worker 池，
到那时 `thread`/`pool` 只保留「历史对照」价值。所以定时器不在它们身上投资。

### `net/` —— 压测与工具

| 文件 | 用途 |
|---|---|
| `bench_server.cpp` | 压测服务端入口，`--server=base\|thread\|pool\|epoll` 运行时切换 |
| `base_echo_server.hpp` | 串行基线（性能零点参照系，仅 test 用） |
| `bench_client.py` | asyncio 压测客户端，`--mode=smoke\|latency\|throughput\|hold` |
| `bench_compare.py` | 四模型 × 三场景横向对比，输出表格/JSON |
| `bigfile_echo.py` | 大文件字节级回显校验 |
| `echo_bytes.py` | 以文件为载荷的逐字节校验（默认 `data/hamlet`，199KB） |
| `handle_write_trigger.py` | 专门触发 `handle_write` / EPOLLOUT 分支 |
| `manual_check_wbuf.py` | 验证 `handle_write` 有没有重复发 wbuf |
| `data/hamlet` | 199,813 B 测试载荷（> 16KB 内核发送缓冲，天生必触发 EAGAIN） |

> `net/` 里的东西**不是测试断言，是测量工具**。它们输出数字，判断留给 `suites/`。

### `debug/` —— 调试脚本

`gdb_epoll_accept.cmds`：

```bash
gdb -q -batch -x test/debug/gdb_epoll_accept.cmds ./build/bench_server
```

### `archive/` —— 历史遗留

| 文件 | 为什么在 archive |
|---|---|
| `frame_test.sh` | 在 `/tmp` 里生成探针代码，且含中文引号/`cosnt` 等错误，从未能编译 |
| `self_test_sticky_package.sh` | grep 一个已被注释掉的 `[recv]` 日志，恒返回 0 行，**测不出任何东西** |

保留是为了不丢历史，**不要新增文件到这里**。

## 一条命令跑全部

```bash
for f in test/suites/*.sh; do
    printf '%-28s ' "$(basename "$f")"
    if bash "$f" >/tmp/t.log 2>&1; then echo PASS; else echo "FAIL (log: /tmp/t.log)"; fi
done
```

## 已知问题

暂无。

### 已修（保留记录，防止回退）

1. ~~`unit/frame_test.cpp` 已过期~~ —— ✅ **已重写**。
   现为 24 B 头 + `TLVDecoder`，96 断言全过，不再自带 `MiniParser`。
2. ~~`bench_client.py` 发裸字节~~ —— ✅ **已加 `--tlv`**。
   `base` 模型用裸字节（`--tlv` 不加），`thread`/`pool`/`epoll` 必须加 `--tlv`。
   `03`、`04`、`05` 里已经改好。
3. ~~`net/bench_compare.py` 不传 `--tlv`~~ —— ✅ **已修**。
   它内部按 `srv.model != "base"` 自动补 `--tlv`，所以四模型对比表可以直接跑：
   ```bash
   python3 test/net/bench_compare.py --models=base,epoll --scenarios=hold
   epoll   OK    1    3816    200      # 修之前这里必然是 DNF（超时）
   ```
   > 修之前的症状：除 `base` 外全部 `DNF`，日志里只有客户端
   > `TimeoutError`——服务端其实一直在正常工作，只是在等 `magic`。

### 待办

1. **`net/bench_compare.py` 的四模型对比表还没全量跑过。**
   参数传递已经正确，缺的是把 `base / thread / pool / epoll × hold / latency / pipeline`
   十二格数据跑满并写进 README 第五节。
   `hold` 场景下 `base` 只能服务 1 条连接（其余排队），解读时别把它当「性能差」，
   它是**串行基线的正确表现**。