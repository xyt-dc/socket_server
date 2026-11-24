# VS Code 里调试 C++（对标 CLion 体验）

## 一、为什么 CLion 不用配、VS Code 要配

| | CLion | VS Code |
|---|---|---|
| 本质 | C++ 专用 IDE | 通用编辑器 |
| 读 CMake | 原生支持 | 靠 CMake Tools 插件 |
| 调试配置 | 自动生成 | 要 `launch.json` |

CLion 把「CMake target → 调试配置」内置了。VS Code 装了 CMake Tools
插件后也能做到 —— 关键是**每个 cpp 都要有对应的 CMake target**。

之前不能这么玩的原因：5 个单测只在 `test/suites/*.sh` 里手写 `g++`
编译，`CMakeLists.txt` 里根本没有它们 → IDE 看不见。

**已修复**：`CMakeLists.txt` 末尾用 `foreach` 把 5 个单测注册成 target。

---

## 二、配一次，永久生效

### 第 1 次（只这一次）

```
Ctrl+Shift+P  →  CMake: Configure
```

选 gcc，生成 `build/` 配置。5 个单测 target 自动出现。

### 每次调试（3 步）

**① 选 target** —— 状态栏点 target 名（或 `Ctrl+Shift+P` →
`CMake: Set Debug Target`）：

```
idle_scanner_test
```

**② 打红点** —— 打开任意源码，行号左侧点一下。
测试文件、`src/timer/*.hpp`、`lib/Connection.hpp` 都能打。

**③ F5** —— 选 `⑥ 调试当前 CMake target（推荐）`。

**换文件调试只改第 ① 步的 target，`launch.json` 完全不动。**

现有 target：

| target | 文件 |
|---|---|
| `blocking_queue_test` | `test/unit/blocking_queue_test.cpp` |
| `frame_test` | `test/unit/frame_test.cpp` |
| `thread_pool_test` | `test/unit/thread_pool_test.cpp` |
| `timer_test` | `test/unit/timer_test.cpp` |
| `idle_scanner_test` | `test/unit/idle_scanner_test.cpp` |
| `bench_server` | `test/net/bench_server.cpp` |
| `socket_server` | `src/main.cpp` |

---

## 三、关键前提：`-g -O0`

```cmake
target_compile_options(${unit_name} PRIVATE -g -O0)
```

| 缺什么 | 后果 |
|---|---|
| 缺 `-g` | 没有行号信息，红点变灰，F5 直接跑完 |
| 开 `-O2` | inline 函数（`touch()`/`collect()`）被内联，断点静默失效 |

现象都一样：**按 F5，程序一闪就结束，什么都没停。**

自查：
```bash
file build/idle_scanner_test | tr ',' '\n' | grep debug_info
# 要有 " with debug_info"
```

---

## 四、停下来之后

| 键 | 作用 |
|---|---|
| `F5` | 继续到下一个断点 |
| `F10` | 单步跳过 |
| `F11` | 单步进入函数内部 |
| `Shift+F11` | 跳出当前函数 |
| `Ctrl+Shift+F5` | 重启（改代码后用，会重新 build） |

看变量三种方式：

1. **VARIABLES 面板** —— 展开 `s` 看 `_timer` / `_timeout` / `_enabled`
2. **鼠标悬停** —— 悬在 `base` 上显示纳秒值
3. **DEBUG CONSOLE** ——

```
-exec print s._timeout
-exec print _map
```

> `-exec` 不能省。cpptools 的 Console 不是纯 gdb，
> 直接敲 `print x` 报 `Unknown command`。

### 最有用：看整张定时器表

断点打在 `src/timer/scan_timer.hpp:67`（`std::vector<Id> fired;`），命中后：

```
-exec print _map
```

```
$1 = std::unordered_map with 1 element = {[7] = ...time_point = { 2696157651916262ns }}
```

`[连接id] = 到期绝对时刻`。

> 为什么不能在 `IdleScanner` 里看：`_timer` 是
> `unique_ptr<TimerQueue>`，静态类型是基类，只能看到 `_vptr`。
> 断到 `ScanTimer::advance` 里，`this` 就是派生类，`_map` 直接可读。

---

## 五、状态栏常用按钮

| 按钮 | 作用 |
|---|---|
| `CMake: [Debug]` | 构建类型 |
| target 名 | 当前**调试**目标 |
| `Build` | 构建当前 target |
| `Run` | 构建并运行（不调试） |

5 个单测已显式加 `-g -O0`，不受 `CMake: [Debug]` 影响。

---

## 六、什么时候还是得命令行

| 场景 | 用什么 |
|---|---|
| 单步看某个函数 | F5 |
| 一次看 100 次调用的完整轨迹 | `gdb -batch -x test/debug/gdb_idle_scanner.cmds` |
| 跑集成测试（06_timer.sh） | 命令行，脚本自己起服务端 |
| CI / 无图形环境 | 命令行 |

调试 `bench_server` 的额外坑：F5 启动后阻塞在 `epoll_wait` 等连接，
**必须另开终端连它**：

```bash
python3 test/net/bench_client.py --port=6401 --mode=smoke --conns=1 --msgs=1
```

且服务端多线程，断点默认只停当前线程。需要在 Call Stack 面板顶部切线程。

**能单测覆盖的逻辑，一律优先在单测里调试。**