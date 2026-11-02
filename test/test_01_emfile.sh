#!/usr/bin/env bash
# =============================================================================
# test_01_emfile.sh
#
# 【测试点 1】文件描述符耗尽（EMFILE）时的行为
#
# 原理
#   服务端用 accept() 取连接。当进程 fd 用光后，accept() 立即返回 -1，
#   errno = EMFILE（Too many open files）。
#
#   正确实现：退避等待（sleep / 预留一个 fd 的经典技巧），日志有节流。
#   当前实现：accept_once() 里 perror() 打日志
#             -> run_loop 里 `if (client_fd < 0) continue;` 立即重试
#             -> 再次 accept 立即失败 -> 空转自旋：CPU 100% + 日志洪水
#
# 判据（观测窗口 2 秒）
#   CPU < 30%  且  日志增长 < 100 行   => PASS
#   CPU >= 30% 或   日志增长 >= 100 行  => FAIL（忙等自旋）
#
# 用法
#   ./test/test_01_emfile.sh
#   PORT=7000 ./test/test_01_emfile.sh
#   FD_LIMIT=32 CONN=64 ./test/test_01_emfile.sh
#   ./test/test_01_emfile.sh --no-build     # 跳过编译，直接用现有二进制
#
# 退出码
#   0 = PASS   1 = FAIL   2 = 环境/构建问题
# =============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/socket_server"

PORT="${PORT:-6101}"
FD_LIMIT="${FD_LIMIT:-24}"   # 服务端可用 fd 上限（含 0/1/2 和 listen fd）
CONN="${CONN:-40}"           # 客户端尝试建立的连接数
HOLD="${HOLD:-5}"            # 客户端保持连接的秒数
WINDOW="${WINDOW:-2}"        # CPU / 日志的观测窗口（秒）
NO_BUILD=0
[ "${1:-}" = "--no-build" ] && NO_BUILD=1

TMPDIR_T="$(mktemp -d)"
LOG="$TMPDIR_T/server.log"
CLIENT_LOG="$TMPDIR_T/client.log"
CLIENT_PY="$TMPDIR_T/client.py"

SERVER_PID=""
CLIENT_PID=""

cleanup() {
    # 先 kill 再 wait，避免 bash 打印 "Killed" 作业通知
    if [ -n "$CLIENT_PID" ]; then kill -9 "$CLIENT_PID" 2>/dev/null; wait "$CLIENT_PID" 2>/dev/null; fi
    if [ -n "$SERVER_PID" ]; then kill -9 "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; fi
    rm -rf "$TMPDIR_T"
}
trap cleanup EXIT INT TERM

hr() { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '  %s\n' "$*"; }

# ---------------------------------------------------------------- 0. 构建
hr
echo "[0/5] 准备可执行文件"
if [ "$NO_BUILD" -eq 1 ]; then
    info "跳过编译（--no-build）"
else
    if ! cmake --build "$ROOT/build" -j >"$TMPDIR_T/build.log" 2>&1; then
        echo "  ✗ 编译失败，先修好代码再来测："
        grep -E 'error' "$TMPDIR_T/build.log" | head -5 | sed 's/^/      /'
        exit 2
    fi
    info "编译通过"
fi
[ -x "$BIN" ] || { echo "  ✗ 找不到可执行文件 $BIN"; exit 2; }
info "二进制: $BIN"

# ------------------------------------------------- 1. 用低 fd 上限启动服务端
hr
echo "[1/5] 以 ulimit -n $FD_LIMIT 启动服务端（端口 $PORT）"
# 在子 shell 里设置 fd 上限 -> exec 后子 shell 的 pid 就是服务端 pid
( ulimit -n "$FD_LIMIT"; exec "$BIN" "$PORT" ) >"$LOG" 2>&1 &
SERVER_PID=$!

# 等待端口真正进入 LISTEN
LISTEN_OK=0
for _ in $(seq 1 50); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
    if ss -ltn 2>/dev/null | grep -q ":$PORT "; then LISTEN_OK=1; break; fi
    sleep 0.1
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "  ✗ 服务端启动即退出"
    sed 's/^/      /' "$LOG"
    exit 2
fi
if [ "$LISTEN_OK" -ne 1 ]; then
    echo "  ⚠ 端口 $PORT 上没有监听（注意：bind 失败时程序会退到随机端口，见测试点 2）"
    echo "  实际监听："
    ss -ltnp 2>/dev/null | grep socket_server | sed 's/^/      /'
    exit 2
fi
info "服务端已监听 :$PORT，pid=$SERVER_PID"

# ------------------------------------------------------- 2. 制造 fd 耗尽
hr
echo "[2/5] 建 $CONN 条连接并保持 $HOLD 秒，把服务端 fd 耗光"

cat >"$CLIENT_PY" <<'PY'
import socket, sys, time
port = int(sys.argv[1]); n = int(sys.argv[2]); hold = float(sys.argv[3])
sk = []
for _ in range(n):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sk.append(s)
    except Exception:
        break
print(len(sk), flush=True)     # 成功建立的连接数
time.sleep(hold)
for s in sk:
    s.close()
PY

python3 "$CLIENT_PY" "$PORT" "$CONN" "$HOLD" >"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!

# 等客户端把连接灌进去
sleep 1.2
CONNECTED="$(head -1 "$CLIENT_LOG" 2>/dev/null || echo 0)"
info "客户端成功建立 $CONNECTED 条连接"

# 只数 socket 类型的 fd：bash 会把若干高位 fd 通过 exec 继承进来，混在总数里会误导
SOCK_N="$(ls -l "/proc/$SERVER_PID/fd" 2>/dev/null | grep -c 'socket:')"
SOCK_MAX="$(awk '/Max open files/{print $4}' "/proc/$SERVER_PID/limits" 2>/dev/null)"
info "服务端 socket fd 数: $SOCK_N   （进程 fd 上限: ${SOCK_MAX:-?}）"

if ! grep -q "Too many open files" "$LOG"; then
    echo "  ⚠ 日志里没有出现 EMFILE，说明 fd 没真正耗尽"
    info "调小 FD_LIMIT 或加大 CONN 后重跑，例如："
    info "  FD_LIMIT=16 CONN=64 $0"
    exit 2
fi
info "已触发 EMFILE（Too many open files）"

# ---------------------------------------------- 3. 观测窗口：CPU + 日志增长
hr
echo "[3/5] 观测 ${WINDOW}s 内的 CPU 占用与日志增长速度"

HZ="$(getconf CLK_TCK)"
[ -z "$HZ" ] && HZ=100

cpu_ticks() { awk '{print $14+$15}' "/proc/$SERVER_PID/stat" 2>/dev/null || echo 0; }

J1="$(cpu_ticks)"
L1="$(wc -l <"$LOG")"
sleep "$WINDOW"
J2="$(cpu_ticks)"
L2="$(wc -l <"$LOG")"

CPU=$(( (J2 - J1) * 100 / (WINDOW * HZ) ))
LOGD=$(( L2 - L1 ))
RATE=$(( LOGD / WINDOW ))

info "CPU        : ${CPU}%   （判据 < 30%）"
info "日志增长   : ${LOGD} 行 / ${WINDOW}s   ≈ ${RATE} 行/秒   （判据 < 100 行）"
info "日志总行数 : ${L2}"

# ------------------------------------------------------------- 4. 结论
hr
echo "[4/5] 判定"
FAIL=0
if [ "$CPU" -ge 30 ]; then
    echo "  ✗ CPU ${CPU}% —— accept 失败后立即重试，忙等自旋"
    FAIL=1
fi
if [ "$LOGD" -ge 100 ]; then
    echo "  ✗ 日志 ${LOGD} 行/${WINDOW}s —— perror 无节流，日志洪水"
    FAIL=1
fi

if [ "$FAIL" -eq 0 ]; then
    echo "  ✓ PASS：accept 失败后有退避，没有自旋"
else
    echo
    echo "  日志末尾采样："
    tail -2 "$LOG" | sed 's/^/      /'
fi

# ------------------------------------------------------------- 5. 证据
hr
echo "[5/5] 关键日志统计"
info "accept failed 出现次数: $(grep -c 'accept failed' "$LOG")"
info "启动输出:"
head -4 "$LOG" | sed 's/^/      /'

hr
if [ "$FAIL" -eq 0 ]; then
    echo "结果: PASS"
    exit 0
else
    echo "结果: FAIL（这是预期中的问题，修复目标见 note/）"
    exit 1
fi
