#!/usr/bin/env bash
# =============================================================================
# test_04_epoll.sh
#
# 【测试点】EpollServer 的正确性 + epoll 模型的核心价值是否成立
#
# 和 test_03 的区别
#   test_03 测的是「缺陷」，PASS = 缺陷被稳定复现。
#   test_04 测的是「正确性」，PASS = 行为符合预期。
#   所以这个脚本第一次跑大概率是 FAIL 的 —— 它就是你的待办清单。
#
# 七个阶段（按依赖顺序，前者失败后者无意义）
#   [1] 功能冒烟    1 连接 1 消息          recv==sent, mismatch==0, errors==0
#   [2] 并发冒烟    50 连接 × 3 消息        conn_ok==50, recv==150
#   [3] fd 回收     并发跑完 fd 数回落       fds_after <= fds_base + 2
#                                            ← 抓 handle_read 缺 n==0 分支
#   [4] 空闲连接    500 连接只连不发         conn_ok==500，且服务端还活着
#   [5] 线程解耦    20 连接 vs 500 连接的线程数必须相等
#                                            ← epoll 的核心价值，note_stage5 §6.4
#   [6] 流水吞吐    20 连接 × 500 消息       partial==0, mismatch==0
#                                            ← 抓 send 半写未处理（wbuf 缺失）
#   [7] 大包压力    20 连接 × 2000 × 4 KB    partial==0 且 RSS 不暴涨
#                    合计 160 MB             ← 唯一能稳定撞出 EAGAIN 的阶段，
#                                            走的是 handle_write 冲刷 wbuf 的路径
#
# 每个阶段都重启服务端，保证 fd 基线干净、失败不互相污染。
#
# 判据全部来自 bench_client.py 的 JSON 字段，不看服务端日志文案。
#
# 用法
#   ./test/test_04_epoll.sh              # 默认：阶段 1 失败就中止（后面的都是连带噪音）
#   ./test/test_04_epoll.sh --all        # 阶段 1 失败也继续跑完，看全局
#   ./test/test_04_epoll.sh --no-build   # 跳过编译
#
# 退出码
#   0 = 全部 PASS
#   1 = 有阶段 FAIL
#   2 = 环境 / 构建 / 服务端起不来
# =============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bench_server"
CLIENT="$ROOT/test/bench_client.py"
MODEL="epoll"

NO_BUILD=0
ALL_MODE=0
for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=1 ;;
        --all)      ALL_MODE=1 ;;
        -h|--help)  sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)          echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

TMPDIR_E="$(mktemp -d)"
SERVER_PID=""
HOLD_PID=""

cleanup() {
    [ -n "$HOLD_PID" ]   && { kill -9 "$HOLD_PID"   2>/dev/null; wait "$HOLD_PID"   2>/dev/null; }
    [ -n "$SERVER_PID" ] && { kill -9 "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; }
    rm -rf "$TMPDIR_E"
}
trap cleanup EXIT INT TERM

hr()   { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '  %s\n' "$*"; }
hdr()  { echo; hr; printf '%s\n' "$*"; }

FAILED=0
ABORT=0
fail() { printf '  ✗ %s\n' "$*"; FAILED=1; }
pass() { printf '  ✓ %s\n' "$*"; }
skip() { printf '  - %s\n' "$*"; }

# ---------------------------------------------------------------- 工具函数

# 启动服务端并等端口可用。$1=port  其余为额外参数
start_server() {
    local port="$1"; shift
    : > "$TMPDIR_E/srv_${port}.log"
    "$BIN" --server="$MODEL" --port="$port" "$@" \
        >"$TMPDIR_E/srv_${port}.log" 2>&1 &
    SERVER_PID=$!

    local i
    for i in $(seq 1 50); do
        # 进程已经死了就不用再等了
        kill -0 "$SERVER_PID" 2>/dev/null || return 1
        if python3 - "$port" <<'PY' 2>/dev/null
import socket, sys
s = socket.socket(); s.settimeout(0.2)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); sys.exit(0)
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
        then
            sleep 0.3   # 探测连接也要被服务端收掉
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

# 服务端还活着吗；死了就把日志打出来
check_alive() {
    kill -0 "$SERVER_PID" 2>/dev/null && return 0
    echo "  ✗ 服务端进程已退出。日志："
    sed 's/^/      /' "$TMPDIR_E"/srv_*.log 2>/dev/null | tail -20
    return 1
}

thread_count() { ls "/proc/$1/task" 2>/dev/null | wc -l | tr -d ' '; }
fd_count()     { ls "/proc/$1/fd"   2>/dev/null | wc -l | tr -d ' '; }

# 从客户端输出里抠 JSON 字段。抠不到返回 -1，绝不抛异常到脚本层。
jget() {  # $1=输出文本  $2=字段名
    printf '%s' "$1" | python3 -c '
import sys, json, re
s = sys.stdin.read()
m = re.search(r"\{.*\}", s, re.S)
if not m:
    print(-1); raise SystemExit
try:
    print(json.loads(m.group(0)).get(sys.argv[1], -1))
except Exception:
    print(-1)
' "$2" 2>/dev/null || echo -1
}

# ---------------------------------------------------------------- 0. 构建
hdr "[0/6] 准备可执行文件"
if [ "$NO_BUILD" -eq 1 ]; then
    [ -x "$BIN" ] || { echo "  ✗ $BIN 不存在，去掉 --no-build"; exit 2; }
    info "跳过编译（--no-build）"
else
    if ! cmake --build "$ROOT/build" --target bench_server -j >"$TMPDIR_E/build.log" 2>&1; then
        echo "  ✗ 编译失败："
        grep -E 'error' "$TMPDIR_E/build.log" | head -10 | sed 's/^/      /'
        exit 2
    fi
    info "编译完成"
fi
info "服务端 = $BIN   (--server=$MODEL)"
info "客户端 = $CLIENT"

# =============================================================================
# [1] 功能冒烟：最小正确性闭环
# =============================================================================
hdr "[1/7] 功能冒烟：1 连接 1 消息"
info "这是最小的正确性闭环。它不通过，后面所有阶段的结果都没有意义。"
echo

P=6401
if ! start_server "$P"; then
    echo "  ✗ 服务端启动失败（端口 $P 起不来）"
    sed 's/^/      /' "$TMPDIR_E/srv_${P}.log"
    exit 2
fi

OUT="$(python3 "$CLIENT" --port="$P" --mode=smoke --conns=1 --msgs=1 --size=64 --json 2>&1)"
echo "$OUT" | sed 's/^/      /'
S1_SENT=$(jget "$OUT" sent)
S1_RECV=$(jget "$OUT" recv)
S1_MIS=$(jget  "$OUT" mismatch)
S1_ERR=$(jget  "$OUT" errors)
S1_EOFE=$(jget "$OUT" eof_early)
stop_server

echo
if [ "$S1_SENT" -ge 1 ] && [ "$S1_RECV" = "$S1_SENT" ] \
   && [ "$S1_MIS" = "0" ] && [ "$S1_ERR" = "0" ]; then
    pass "单连接 echo 闭环成立（sent=$S1_SENT, recv=$S1_RECV）"
else
    fail "单连接 echo 未闭环（sent=$S1_SENT, recv=$S1_RECV, errors=$S1_ERR, eof_early=$S1_EOFE）"
    ABORT=1
fi

if [ "$ABORT" -eq 1 ] && [ "$ALL_MODE" -eq 0 ]; then
    echo
    info "阶段 1 失败 → 中止。后面六个阶段此时全是连带噪音。"
    info "想看完整体表现加 --all。"
    echo
    info "先查这两条（按命中率排序）："
    info "  ① listen fd 是否阻塞 —— accept4 的 SOCK_NONBLOCK 只作用于新 fd，"
    info "     不改 listen fd 本身。它阻塞的话 reactor 第一次 accept 之后"
    info "     就再也不会回到 epoll_wait，表现是「连上了、发了、没回音」。"
    info "     修法：epoll_init() 里 add(_listen_fd) 之前 fcntl 加 O_NONBLOCK。"
    info "  ② epoll_init() 是否返回 false —— 那 run_loop 会直接 return，"
    info "     start() 跟着返回，进程静默退出。"
    echo
    hr
    printf '%s\n' "  ✗ ABORT —— 先修阶段 1"
    exit 1
fi

# =============================================================================
# [2] 并发冒烟 + [3] fd 回收
# =============================================================================
hdr "[2/7] 并发冒烟：50 连接 × 3 消息"
info "同时验证「能接受并发」和「能正确回声」。"
echo

P=6402
if ! start_server "$P"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    sed 's/^/      /' "$TMPDIR_E/srv_${P}.log"
    exit 2
fi
FD_BASE=$(fd_count "$SERVER_PID")
info "fd 基线 = $FD_BASE（stdin/out/err + listen + epoll）"
echo

OUT="$(python3 "$CLIENT" --port="$P" --mode=smoke --conns=50 --msgs=3 --size=64 --json 2>&1)"
echo "$OUT" | sed 's/^/      /'
S2_OK=$(jget   "$OUT" conn_ok)
S2_SENT=$(jget "$OUT" sent)
S2_RECV=$(jget "$OUT" recv)
S2_MIS=$(jget  "$OUT" mismatch)
S2_ERR=$(jget  "$OUT" errors)
S2_REF=$(jget  "$OUT" refused)

echo
if [ "$S2_OK" = "50" ] && [ "$S2_REF" = "0" ]; then
    pass "50 条连接全部建立（refused=0）"
elif [ "$S2_REF" != "0" ]; then
    fail "有连接被拒（refused=$S2_REF）—— 检查 listen(fd, 5) 的 backlog，建议 SOMAXCONN"
else
    fail "只建立了 $S2_OK/50 条连接"
fi

if [ "$S2_SENT" = "150" ] && [ "$S2_RECV" = "150" ] \
   && [ "$S2_MIS" = "0" ] && [ "$S2_ERR" = "0" ]; then
    pass "150 条消息全部正确回声（mismatch=0, errors=0）"
else
    fail "消息不完整（sent=$S2_SENT, recv=$S2_RECV, mismatch=$S2_MIS, errors=$S2_ERR）"
fi

# --- [3] fd 回收 ---
hdr "[3/7] fd 回收：连接关闭后 fd 数是否回落"
info "客户端 50 条连接全部关闭后，服务端应当回收这 50 个 fd。"
echo

sleep 1.5   # 给 reactor 时间处理 FIN
if ! check_alive; then
    fail "服务端在 50 连接压测后退出了"
else
    FD_AFTER=$(fd_count "$SERVER_PID")
    info "基线 $FD_BASE → 现在 $FD_AFTER"
    echo
    if [ "$FD_AFTER" -le $((FD_BASE + 2)) ]; then
        pass "fd 已全部回收（$FD_AFTER <= $FD_BASE+2）"
    else
        fail "fd 泄漏：多出 $((FD_AFTER - FD_BASE)) 个（基线 $FD_BASE → $FD_AFTER）"
        info "典型根因：handle_read 缺 n==0 分支。"
        info "对端 FIN 时 recv 返回 0，现在会掉进 else 里去读一个过期的 errno："
        info "  残留 EINTR → 死循环；残留 EAGAIN → return 且不关连接 → fd 泄漏。"
        info "修法：if (n == 0) { del_and_close(fd); return; }"
    fi
fi
stop_server

# =============================================================================
# [4] 空闲连接：epoll 的主场
# =============================================================================
hdr "[4/7] 空闲连接：500 条只连不发"
info "这是 epoll 相对线程模型的本质优势，也是 note_stage5 §6.4 的验收项。"
echo

P=6403
if ! start_server "$P"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    sed 's/^/      /' "$TMPDIR_E/srv_${P}.log"
    exit 2
fi

HOLD_CONNS=500
python3 "$CLIENT" --port="$P" --mode=hold --conns="$HOLD_CONNS" --hold=6 \
    --json >"$TMPDIR_E/hold500.log" 2>&1 &
HOLD_PID=$!
sleep 3   # 等连接全部落到服务端

# 此时客户端还没跑完（hold=6s），JSON 尚未输出，只能先采进程指标
if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    N_THREAD_500=$(thread_count "$SERVER_PID")
    FD_HOLD=$(fd_count "$SERVER_PID")
else
    N_THREAD_500=-1
    FD_HOLD=-1
fi

# 等客户端自己跑完（它会正常 close 并打印 JSON），而不是 kill 掉
wait "$HOLD_PID" 2>/dev/null
HOLD_PID=""

OUT="$(cat "$TMPDIR_E/hold500.log")"
S4_OK=$(jget "$OUT" conn_ok)
S4_FAIL=$(jget "$OUT" conn_fail)
S4_REF=$(jget "$OUT" refused)

info "conn_ok=$S4_OK  conn_fail=$S4_FAIL  refused=$S4_REF"
info "服务端此时：线程数=$N_THREAD_500  fd 数=$FD_HOLD"
echo
if [ "$S4_OK" = "$HOLD_CONNS" ]; then
    pass "$HOLD_CONNS 条空闲连接全部建立"
else
    fail "只建立了 $S4_OK/$HOLD_CONNS 条空闲连接（refused=$S4_REF）"
    info "refused>0 → 还是 backlog 太小；conn_fail>0 → 先看服务端日志"
fi

if [ "$FD_HOLD" -ge $((HOLD_CONNS + 2)) ]; then
    pass "服务端 fd 数 $FD_HOLD 与连接数一致（确实是 500 条同时在服务）"
else
    fail "fd 数 $FD_HOLD 远小于 $HOLD_CONNS —— 连接没有被真正持有"
fi
stop_server

# =============================================================================
# [5] 线程数解耦：epoll 的核心价值
# =============================================================================
hdr "[5/7] 线程数与连接数解耦"
info "同样是 hold 模式，只改连接数，看服务端线程数是否变化。"
info "要求：20 条和 500 条连接的线程数必须【相等】。"
echo

measure_threads() {  # $1=port $2=conns → 输出线程数
    local port="$1" conns="$2"
    python3 "$CLIENT" --port="$port" --mode=hold --conns="$conns" --hold=8 \
        >/dev/null 2>&1 &
    HOLD_PID=$!
    sleep 2.5
    local n
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        n=$(thread_count "$SERVER_PID")
    else
        n=-1
    fi
    kill -9 "$HOLD_PID" 2>/dev/null; wait "$HOLD_PID" 2>/dev/null; HOLD_PID=""
    echo "$n"
}

P=6404
if ! start_server "$P"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    exit 2
fi
N_SMALL=$(measure_threads "$P" 20)
N_BIG=$(measure_threads "$P" 500)
stop_server

info "20  条连接 → $N_SMALL 个线程"
info "500 条连接 → $N_BIG 个线程"
echo
if [ "$N_SMALL" = "$N_BIG" ] && [ "$N_BIG" -gt 0 ]; then
    pass "线程数与连接数解耦（都是 $N_BIG 个）"
    info "对照 test_03 实验 C：thread 模型 20 连接 = 21 线程，pool(workers=4) = 5 线程。"
    info "epoll 模型 = $N_BIG 线程，且与连接数无关。"
else
    fail "线程数随连接数增长（20→$N_SMALL, 500→$N_BIG）"
    info "epoll 用在单线程 reactor 上，不该出现这种变化。"
    info "检查是否已经把 IoThreadPool 接进来了（_pool.try_run_one()），"
    info "或者 hold 模式的连接触发了额外线程创建。"
fi

# =============================================================================
# [6] 流水线吞吐：抓 send 半写
# =============================================================================
hdr "[6/7] 流水线吞吐：20 连接 × 500 消息"
info "客户端连续灌 500 条不等回复，专门暴露「send 遇 EAGAIN 直接丢数据」。"
info "partial>0 或 mismatch>0 就是 wbuf + EPOLLOUT 还没做的证据。"
echo

P=6405
if ! start_server "$P"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    exit 2
fi

OUT="$(python3 "$CLIENT" --port="$P" --mode=throughput --conns=20 --msgs=500 \
        --size=64 --json 2>&1)"
echo "$OUT" | sed 's/^/      /'
S6_RECV=$(jget "$OUT" recv)
S6_MIS=$(jget  "$OUT" mismatch)
S6_ERR=$(jget  "$OUT" errors)
S6_PART=$(jget "$OUT" partial)
S6_MPS=$(jget  "$OUT" msg_per_s)
if kill -0 "$SERVER_PID" 2>/dev/null; then
    FD_TP=$(fd_count "$SERVER_PID")
else
    FD_TP=-1
fi
stop_server

echo
if [ "$S6_PART" = "0" ] && [ "$S6_MIS" = "0" ] && [ "$S6_ERR" = "0" ]; then
    pass "10000 条消息无截断（partial=0, mismatch=0, errors=0）"
    info "吞吐 $S6_MPS msg/s"
else
    fail "数据完整性有问题（partial=$S6_PART, mismatch=$S6_MIS, errors=$S6_ERR, recv=$S6_RECV）"
    if [ "$S6_ERR" -gt 0 ] && [ "$S6_RECV" = "0" ]; then
        info "recv=0 → 一条都没回来，先修阶段 1（reactor 根本没在跑）。"
    elif [ "$S6_PART" -gt 0 ]; then
        info "partial>0 = 服务端发出去了但没发完 → send 遇 EAGAIN 直接 return。"
        info "修法：Connection 加 wbuf，_msg_cb 只追加不发送，"
        info "      追加后尝试 flush，EAGAIN 就 epoll.mod(fd, EPOLLIN|EPOLLOUT)，"
        info "      handle_write 在 EPOLLOUT 时把 wbuf 冲干净再 mod 回 EPOLLIN。"
        info "注意：这也是 TLS 的硬前置条件（SSL_write 同样会 WANT_WRITE）。"
    elif [ "$S6_MIS" -gt 0 ]; then
        info "mismatch>0 且 partial=0 → 数据长度对但内容不对。"
        info "通常是「写一半读一半」的环形缓冲错位，或粘包未分帧。"
    fi
fi

# =============================================================================
# [7] 大包压力：唯一能稳定撞出 EAGAIN 的阶段
# =============================================================================
hdr "[7/7] 大包压力：20 连接 × 2000 消息 × 4 KB"
info "合计 160 MB。阶段 6 只有 1.28 MB，内核发送缓冲根本填不满，"
info "所以 handle_write / wbuf 那条路径一次都不会被走到。"
info "本阶段才真正压到「send 半写 → append_wbuf → EPOLLOUT → handle_write」。"
echo

P=6406
if ! start_server "$P"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    sed 's/^/      /' "$TMPDIR_E/srv_${P}.log"
    exit 2
fi

RSS_BEFORE=$(awk '/VmRSS/{print $2}' "/proc/$SERVER_PID/status" 2>/dev/null)
S7_CONNS=20
S7_MSGS=2000
S7_SIZE=4096

OUT="$(python3 "$CLIENT" --port="$P" --mode=throughput --conns="$S7_CONNS" \
        --msgs="$S7_MSGS" --size="$S7_SIZE" --json 2>&1)"
echo "$OUT" | sed 's/^/      /'
S7_SENT=$(jget "$OUT" sent)
S7_RECV=$(jget "$OUT" recv)
S7_MIS=$(jget  "$OUT" mismatch)
S7_ERR=$(jget  "$OUT" errors)
S7_PART=$(jget "$OUT" partial)
S7_EOFE=$(jget "$OUT" eof_early)
S7_MPS=$(jget  "$OUT" msg_per_s)
S7_MIB=$(jget  "$OUT" mib_per_s)
S7_ELAP=$(jget "$OUT" elapsed_s)

if kill -0 "$SERVER_PID" 2>/dev/null; then
    check_alive || true
    RSS_AFTER=$(awk '/VmRSS/{print $2}' "/proc/$SERVER_PID/status" 2>/dev/null)
else
    RSS_AFTER=-1
fi
stop_server

EXPECTED_MSGS=$((S7_CONNS * S7_MSGS))
echo
if [ "$S7_SENT" = "$EXPECTED_MSGS" ] && [ "$S7_RECV" = "$EXPECTED_MSGS" ] \
   && [ "$S7_MIS" = "0" ] && [ "$S7_ERR" = "0" ] \
   && [ "$S7_PART" = "0" ] && [ "$S7_EOFE" = "0" ]; then
    pass "$EXPECTED_MSGS 条 × ${S7_SIZE}B = $((EXPECTED_MSGS * S7_SIZE / 1048576)) MB 全部正确回显"
    info "吞吐 $S7_MPS msg/s = $S7_MIB MiB/s，耗时 ${S7_ELAP}s"
else
    fail "大包压力下数据不完整"
    info "sent=$S7_SENT recv=$S7_RECV mismatch=$S7_MIS errors=$S7_ERR partial=$S7_PART eof_early=$S7_EOFE"
    if [ "$S7_PART" -gt 0 ]; then
        info "partial>0 → wbuf 里存了数据但没被冲刷完，连接就被关了。"
        info "  查三处：① flush() 遇 EAGAIN 有没有 append_wbuf + mod(IN|OUT)"
        info "          ② handle_write 有没有 while(!wbuf_empty()) 循环继续 send"
        info "          ③ 每条成功 send 后有没有 wbuf_retrieve(n) 推进偏移"
        info "  ③ 漏了 → 同一段数据反复重发，客户端看到的字节数会远超预期。"
    fi
    if [ "$S7_ERR" -gt 0 ] && [ "$S7_RECV" = "0" ]; then
        info "recv=0 → 服务端在大包下直接崩了。先看 check_alive 打印的日志。"
    fi
fi

# --- RSS：wbuf 是否被正确释放 ---
# 160 MB 流过 20 条连接。若 wbuf_retrieve 没把已发数据弹出去，
# 每条连接的 _wbuf 会一路涨到 8 MB，20 条就是 160 MB 常驻。
RSS_LIMIT=51200   # 50 MB，正常实现峰值在 5~10 MB
if [ "$RSS_AFTER" -gt 0 ] && [ "$RSS_BEFORE" -gt 0 ]; then
    RSS_GROW=$((RSS_AFTER - RSS_BEFORE))
    info "服务端 RSS：${RSS_BEFORE}KB → ${RSS_AFTER}KB（增长 ${RSS_GROW}KB）"
    echo
    if [ "$RSS_AFTER" -le "$RSS_LIMIT" ]; then
        pass "内存没有随数据量线性增长（${RSS_AFTER}KB < ${RSS_LIMIT}KB）"
    else
        fail "RSS 涨到 ${RSS_AFTER}KB —— wbuf 疑似没被释放"
        info "160 MB 数据流过后内存应回落。涨到几十 MB 以上说明："
        info "  · wbuf_retrieve 没被调用，或调用了但没真的 erase 掉前缀"
        info "  · 或 flush() 的 early return 让 wbuf 只进不出"
        info "修法：handle_write 里每 send 成功 n 字节就 wbuf_retrieve(n)，"
        info "      wbuf 清空后 mod(fd, EPOLLIN) 摘掉 EPOLLOUT。"
    fi
else
    skip "服务端进程已不在，无法采 RSS"
fi

# =============================================================================
# 汇总
# =============================================================================
hdr "[8/8] 汇总"

if [ "$FAILED" -eq 0 ]; then
    echo "  ✓ PASS —— EpollServer 七个维度全部正常"
    echo
    echo "  epoll 这一章可以收尾了。可以写进 README §五 证据表的数据："
    echo "    · ${HOLD_CONNS} 条空闲连接 / ${N_BIG} 个线程 / fd 数正确"
    echo "    · 20×500 流水线 partial=0"
    echo
    echo "  下一步（按优先级）："
    echo "    1. handle_write + EPOLLOUT + wbuf   ← 若阶段 6 已经 PASS 可仍要做，"
    echo "       因为低配 / 高延迟链路下 EAGAIN 才会真的出现，本机 loopback 测不出来"
    echo "    2. eventfd 唤醒 epoll_wait，告别 100ms 退出延迟"
    echo "    3. TCP_NODELAY（不做会看到 ~40ms Nagle 延迟）"
    echo "    4. idle 超时（slowloris 防御），epoll_wait 超时改「下一个定时器到期」"
    echo "    5. main-sub reactor 多线程化"
    hr
    exit 0
else
    echo "  ✗ FAIL —— 有阶段未通过，见上面 ✗ 标记"
    echo
    echo "  症状 → 根因 速查："
    echo "    recv=0 errors=N 全部超时   → listen fd 阻塞，reactor 卡在 accept"
    echo "    refused>0                  → listen backlog 太小（5 → SOMAXCONN）"
    echo "    fd 数只增不减              → handle_read 缺 n==0 分支"
    echo "    partial>0                  → send 半写未处理（缺 wbuf + EPOLLOUT）"
    echo "    p99 ≈ 100ms 的整数倍       → 事件没被唤醒，靠 epoll_wait 超时轮询"
    hr
    exit 1
fi