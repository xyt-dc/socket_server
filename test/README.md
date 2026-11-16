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

### `suites/` —— 端到端测试套件

统一约定：`set -uo pipefail` + `TMPDIR` + `trap cleanup` +
`退出码 0=PASS 1=FAIL 2=环境问题`。

| 脚本 | 测什么 | 依赖 |
|---|---|---|
| `01_emfile.sh` | `accept()` 遇到 EMFILE 时会不会自旋烧 CPU | `socket_server` |
| `02_blocking_queue.sh` | `BlockingQueue` 功能 + `-O2` + TSan 三档 | `unit/blocking_queue_test.cpp` |
| `03_pool_starvation.sh` | 线程池 worker 被占满后的饿死 + 背压 | `bench_server` `bench_client.py` |
| `04_epoll.sh` | epoll 模型七阶段（冒烟 / 粘包 / 大文件 / wbuf …） | `bench_server` `bench_client.py` |
| `05_tlv.sh` | **TLV 协议链路**：往返 / seq 回填 / flag / 粘包 / 拆包 / 坏帧 | `bench_server` `tlv_probe.py` |

```bash
./test/suites/05_tlv.sh                     # 默认 pool 模型
./test/suites/05_tlv.sh --server=epoll      # 换模型
./test/suites/05_tlv.sh --strict            # 坏帧不断连也判 FAIL
```

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
   `03`、`04`、`05` 里已经改好，`bench_compare.py` 仍需手动传参。

### 待办

1. **`net/bench_compare.py` 的四模型对比表还没跑过。**
   它调 `bench_client.py` 时需要区分 `base`（裸字节）与其余三个（`--tlv`），
   否则 `base` 的对比数据会因协议不同而不公平。