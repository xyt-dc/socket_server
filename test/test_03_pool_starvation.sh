#!/usr/bin/env bash
# =============================================================================
# test_03_pool_starvation.sh
#
# 【测试点】多线程 + 消息队列方案的并发度上限在哪里
#
# 核心命题
#   当前 ThreadPool_server 的 submit() 提交的是 handle_client_read(fd)，
#   这是一个「整个连接生命周期内不返回」的长任务 —— 它在 recv() 上阻塞。
#   于是：一个 worker 一旦被分配了连接，就再也不会回到队列。
#
#   推论：并发服务能力 == worker 数量，与 fd 上限无关。
#         多出来的连接 accept 成功、submit 成功、进了 _clients，
#         但永远没有 worker 去看它 —— 客户端发了数据永远收不到回应。
#
# 三个实验
#   A. 饿死实验  workers=2，先用 2 条「只连不发」的连接占满 worker，
#                再建第 3 条连接发请求 —— 用 thread 模型做对照。
#                预期：pool 超时无响应，thread 正常响应。
#
#   B. 队满实验  workers=1 + queue-cap=1，占满 worker 再塞满队列，
#                第 3 条连接会被 submit 返回 false 直接 close。
#                预期：客户端读到 EOF（连接被服务端关掉）。
#
#   C. 线程爆炸  统计两种模型在不同连接数下的 OS 线程数。
#                预期：thread 模型线程数 = 连接数 + 1；pool 模型恒等于 workers + 1。
#
# 判据（不看日志文案，只看行为）
#   A: thread 模型 msg 收全 且 pool 模型 errors>0  => PASS（缺陷被复现）
#   B: 客户端出现 EOF 或错误                        => PASS
#   C: 数字对比即可，无数值断言
#
# 用法
#   ./test/test_03_pool_starvation.sh
#   ./test/test_03_pool_starvation.sh --no-build
#
# 退出码
#   0 = 全部 PASS（缺陷被稳定复现）
#   1 = FAIL（预期现象没出现 —— 说明代码变了，结论要重新评估）
#   2 = 环境/构建问题
# =============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bench_server"
CLIENT="$ROOT/test/bench_client.py"

NO_BUILD=0
[ "${1:-}" = "--no-build" ] && NO_BUILD=1

TMPDIR_T="$(mktemp -d)"
SERVER_PID=""
HOLD_PID=""

cleanup() {
    if [ -n "$HOLD_PID" ];   then kill -9 "$HOLD_PID"   2>/dev/null; wait "$HOLD_PID"   2>/dev/null; fi
    if [ -n "$SERVER_PID" ]; then kill -9 "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; fi
    rm -rf "$TMPDIR_T"
}
trap cleanup EXIT INT TERM

hr()   { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '  %s\n' "$*"; }
hdr()  { hr; printf '%s\n' "$*"; }

FAILED=0
fail() { printf '  ✗ %s\n' "$*"; FAILED=1; }
pass() { printf '  ✓ %s\n' "$*"; }

# ---------------------------------------------------------------- 工具函数

# 启动服务端并等端口可用。$1=model $2=port 其余为额外参数
start_server() {
    local model="$1" port="$2"; shift 2
    : > "$TMPDIR_T/server_${port}.log"
    "$BIN" --server="$model" --port="$port" "$@" \
        >"$TMPDIR_T/server_${port}.log" 2>&1 &
    SERVER_PID=$!

    # 最多等 5 秒，直到能连上
    local i
    for i in $(seq 1 50); do
        if python3 - "$port" <<'PY' 2>/dev/null
import socket, sys
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(("127.0.0.1", int(sys.argv[1])))
    sys.exit(0)
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
        then
            # 上面这个探测连接本身会占一个 fd，等它被服务端收掉再返回
            sleep 0.3
            return 0
        fi
        sleep 0.1
    done
    return 1
}

stop_server() {
    if [ -n "$SERVER_PID" ]; then
        kill -INT "$SERVER_PID" 2>/dev/null
        local i
        for i in $(seq 1 30); do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
        SERVER_PID=""
    fi
}

thread_count() {
    ls "/proc/$1/task" 2>/dev/null | wc -l
}

# ---------------------------------------------------------------- 0. 构建
hdr "[0/4] 准备可执行文件"
if [ "$NO_BUILD" -eq 1 ]; then
    [ -x "$BIN" ] || { echo "  ✗ $BIN 不存在，去掉 --no-build"; exit 2; }
    info "跳过编译（--no-build）"
else
    if ! cmake --build "$ROOT/build" --target bench_server -j >"$TMPDIR_T/build.log" 2>&1; then
        echo "  ✗ 编译失败："
        grep -E 'error' "$TMPDIR_T/build.log" | head -10 | sed 's/^/      /'
        exit 2
    fi
    info "编译完成"
fi
info "服务端 = $BIN"
info "客户端 = $CLIENT"

# =============================================================================
# 实验 A：worker 被长连接占满后，新连接彻底饿死
# =============================================================================
hdr "[1/4] 实验 A：worker 占满后的饿死现象"
info "场景：workers=2，先建 2 条只连不发的连接，再建第 3 条发请求"
echo

run_starvation() {
    local model="$1" port="$2" workers="$3"
    local extra=()
    [ "$model" = "pool" ] && extra=(--workers="$workers")

    if ! start_server "$model" "$port" "${extra[@]+"${extra[@]}"}"; then
        echo "  ✗ 服务端 $model 启动失败"
        cat "$TMPDIR_T/server_${port}.log" | sed 's/^/      /'
        return 1
    fi

    # 2 条连接只连不发，hold 8 秒
    python3 "$CLIENT" --port="$port" --mode=hold --conns=2 --hold=8 \
        >"$TMPDIR_T/hold_${model}.log" 2>&1 &
    HOLD_PID=$!
    sleep 1.5   # 等这 2 条连接真正落到 worker 上

    # 第 3 条连接：发 3 条 64 字节消息，看有没有回应
    local out
    out="$(python3 "$CLIENT" --port="$port" --mode=smoke \
            --conns=1 --msgs=3 --size=64 --json 2>&1)"
    local rc=$?

    kill -9 "$HOLD_PID" 2>/dev/null; wait "$HOLD_PID" 2>/dev/null; HOLD_PID=""

    echo "  ── $model 模型 ──"
    echo "$out" | sed 's/^/      /'
    stop_server
    return $rc
}

POOL_OUT="$(run_starvation pool 6211 2)"
POOL_RC=$?
echo
THREAD_OUT="$(run_starvation thread 6212 0)"
THREAD_RC=$?
echo

# 判定：pool 模型第 3 条连接收不到 echo（errors>0 或 recv<sent）
POOL_RECV=$(echo "$POOL_OUT" | python3 -c 'import sys,json,re
s=sys.stdin.read()
m=re.search(r"\{.*\}",s,re.S)
print(json.loads(m.group(0))["recv"] if m else -1)' 2>/dev/null)
POOL_ERR=$(echo "$POOL_OUT" | python3 -c 'import sys,json,re
s=sys.stdin.read()
m=re.search(r"\{.*\}",s,re.S)
print(json.loads(m.group(0))["errors"] if m else -1)' 2>/dev/null)
THREAD_RECV=$(echo "$THREAD_OUT" | python3 -c 'import sys,json,re
s=sys.stdin.read()
m=re.search(r"\{.*\}",s,re.S)
print(json.loads(m.group(0))["recv"] if m else -1)' 2>/dev/null)

echo "  ── 结论 ──"
info "pool  模型：第 3 条连接收到 $POOL_RECV 条 echo，errors=$POOL_ERR"
info "thread 模型：第 3 条连接收到 $THREAD_RECV 条 echo"

if [ "$POOL_RECV" = "0" ] || [ "$POOL_ERR" != "0" ]; then
    pass "pool 模型复现饿死：worker 被占满后新连接无响应"
else
    fail "pool 模型没有饿死（recv=$POOL_RECV）—— 代码可能已改动"
fi

if [ "$THREAD_RECV" = "3" ]; then
    pass "thread 模型正常服务第 3 条连接（对照成立）"
else
    fail "thread 模型也异常（recv=$THREAD_RECV）—— 对照组不可用"
fi

# =============================================================================
# 实验 B：队列满 → 拒绝连接
# =============================================================================
hdr "[2/4] 实验 B：队列满时的背压行为"
info "场景：workers=1 queue-cap=1，占满 worker + 塞满队列，第 3 条连接被拒"
echo

if start_server pool 6213 --workers=1 --queue-cap=1; then
    python3 "$CLIENT" --port=6213 --mode=hold --conns=1 --hold=8 \
        >"$TMPDIR_T/b_hold.log" 2>&1 &
    HOLD_PID=$!
    sleep 1.5   # worker 被占住

    # 这条进的队列，也没人跑它
    python3 "$CLIENT" --port=6213 --mode=hold --conns=1 --hold=8 \
        >"$TMPDIR_T/b_hold2.log" 2>&1 &
    HOLD2_PID=$!
    sleep 1.5   # 队列被占满

    # 第 3 条：submit 应该失败 → 服务端 close(fd) → 客户端立刻 EOF
    B_OUT="$(python3 "$CLIENT" --port=6213 --mode=smoke --conns=1 --msgs=1 \
              --size=64 --json 2>&1)"
    echo "$B_OUT" | sed 's/^/      /'

    EOF_B=$(echo "$B_OUT" | python3 -c 'import sys,json,re
s=sys.stdin.read()
m=re.search(r"\{.*\}",s,re.S)
print(json.loads(m.group(0))["eof_early"] if m else -1)' 2>/dev/null)
    RECV_B=$(echo "$B_OUT" | python3 -c 'import sys,json,re
s=sys.stdin.read()
m=re.search(r"\{.*\}",s,re.S)
print(json.loads(m.group(0))["recv"] if m else -1)' 2>/dev/null)

    kill -9 "$HOLD_PID" "$HOLD2_PID" 2>/dev/null
    wait "$HOLD_PID" "$HOLD2_PID" 2>/dev/null
    HOLD_PID=""
    stop_server

    echo
    if [ "$RECV_B" = "0" ] && [ "$EOF_B" = "1" ]; then
        pass "第 3 条连接被服务端主动 close（eof_early=1, recv=0）"
        info "这是「拒绝连接」：submit 失败 → 立刻 close(fd)"
        info "注意它不是「限流」—— 前 2 条连接一条都不会被减速，"
        info "服务端对已经 accept 进来的快发送方毫无办法"
    elif [ "$RECV_B" = "0" ]; then
        pass "第 3 条连接拿不到服务（recv=0, eof_early=$EOF_B）"
    else
        fail "队列满时第 3 条连接仍被服务（recv=$RECV_B）"
    fi
else
    fail "服务端启动失败"
fi

# =============================================================================
# 实验 C：线程数随连接数增长
# =============================================================================
hdr "[3/4] 实验 C：两种模型的 OS 线程数"
info "每条连接的服务开销，直接看 /proc/PID/task 数量"
echo

C_CONNS=20
if start_server thread 6214; then
    python3 "$CLIENT" --port=6214 --mode=hold --conns="$C_CONNS" --hold=6 \
        >/dev/null 2>&1 &
    HOLD_PID=$!
    sleep 2
    N_THREAD=$(thread_count "$SERVER_PID")
    info "thread 模型，$C_CONNS 条连接 → $N_THREAD 个线程"
    kill -9 "$HOLD_PID" 2>/dev/null; wait "$HOLD_PID" 2>/dev/null; HOLD_PID=""
    stop_server
fi

if start_server pool 6215 --workers=4 --queue-cap=1024; then
    python3 "$CLIENT" --port=6215 --mode=hold --conns="$C_CONNS" --hold=6 \
        >/dev/null 2>&1 &
    HOLD_PID=$!
    sleep 2
    N_POOL_THREAD=$(thread_count "$SERVER_PID")
    info "pool   模型（workers=4），$C_CONNS 条连接 → $N_POOL_THREAD 个线程"
    kill -9 "$HOLD_PID" 2>/dev/null; wait "$HOLD_PID" 2>/dev/null; HOLD_PID=""
    stop_server
fi

echo
info "thread 模型：线程数 ≈ 连接数 + 1"
info "pool   模型：线程数 ≈ workers + 1（连接数无关）"
info "→ pool 省下了线程，代价是并发度被 worker 数锁死"
info "→ 这也正是 epoll 要解决的：把「一个线程服务一条连接」"
info "   换成「一个线程管理一万条连接」，线程数不再随连接数增长"
echo

# =============================================================================
# 汇总
# =============================================================================
hdr "[4/4] 汇总"
if [ "$FAILED" -eq 0 ]; then
    echo "  ✓ PASS —— 三个缺陷都被复现"
    echo
    echo "  缺陷清单："
    echo "    1. 并发度上限 = worker 数（阻塞 recv 焊死 worker）"
    echo "    2. 队列满只能拒绝新连接，无法对已有连接限流"
    echo "    3. 线程数与连接数的关系没解决，只是把「爆炸」换成「封顶」"
    exit 0
else
    echo "  ✗ FAIL —— 预期现象没出现，结论需要重新评估"
    exit 1
fi