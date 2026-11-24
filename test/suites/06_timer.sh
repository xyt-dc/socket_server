#!/usr/bin/env bash
# =============================================================================
# 06_timer.sh
#
# 【测试点】空闲超时（idle timeout）—— 连接生命周期的策略，不是协议正确性
#
# 和 04_epoll.sh 的分工
#   04_epoll 证明「epoll 能扛住 500 条空闲连接」。
#   06_timer 证明「扛不住的那些能被清掉，而且只清该清的」。
#
# 为什么这一章不能只看「断开成功了」
#   `close()` 一行就能让连接消失，但错误的实现同样能让它消失：
#     · 用 system_clock 计时 → NTP 校时后集体误杀，平时测不出来
#     · 只在 recv 时刷时间戳 → 只推不读的连接被误杀
#     · 用「上次扫描时间」而不是「上次活跃时间」→ 边界漂移，时快时慢
#     · 遍历 _conns 时 erase → 迭代器失效，偶发崩溃或漏删
#   所以这一章必须正反两面都测：该杀的杀掉（die），不该杀的留住（alive）。
#
# 适用范围：只测 epoll（以及未来在这个基础上长出来的主从 reactor）
#   thread（one-per-conn）是基准实现，不做定时器。
#   pool 是第三种服务器的过渡板，最终会被吸收进主从 reactor 的 worker 池，
#   也不单独做定时器。
#   两者共同点是 client fd 来自阻塞的 accept()，收尾要靠 shutdown 唤醒 worker；
#   而 epoll / reactor 的 client fd 来自 accept4(SOCK_NONBLOCK)，
#   单线程内 del_and_close 就是全部动作，没有这种困扰。
#
# 四个阶段（按依赖顺序）
#   [1] 该杀的杀掉     连上不说          服务端应主动断开（FIN 或 RST 都算）
#   [2] 不该杀的留住   每 0.5s 发一次     连续 8s（4× 超时）一次都不能断
#                                          ← 抓「刷新点漏了 send 成功」
#   [3] 混合场景       1 条死 + 5 条活共处同一 epoll
#                      死的被清、活的零误伤、进程不崩
#                                          ← 抓「遍历中 erase / 持锁删除」
#   [4] 阈值边界       按 0.75× 超时的间隔持续发
#                                          ← 抓「基准取了上次扫描时间」
#
# 用法
#   ./test/suites/06_timer.sh              # 全量
#   ./test/suites/06_timer.sh --no-build   # 跳过编译
#   ./test/suites/06_timer.sh --keep       # 保留临时目录（看服务端日志）
#   ./test/suites/06_timer.sh --server=reactor
#                                          # 换被测模型（默 epoll）
#                                          # 主从 reactor 落地后直接传它，
#                                          # 本脚本其余部分无需改动
#
# 退出码
#   0 = 全部 PASS
#   1 = 有阶段 FAIL
#   2 = 环境 / 构建 / 服务端起不来
# =============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/build/bench_server"
PROBE="$ROOT/test/net/idle_probe.py"

NO_BUILD=0
KEEP=0
MODEL="epoll"
for arg in "$@"; do
    case "$arg" in
        --no-build)     NO_BUILD=1 ;;
        --keep)         KEEP=1 ;;
        --server=*)     MODEL="${arg#--server=}" ;;
        -h|--help)      sed -n '2,50p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)              echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

TMPDIR_T="$(mktemp -d)"
SERVER_PID=""

cleanup() {
    [ -n "$SERVER_PID" ] && { kill -9 "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; }
    if [ "$KEEP" -eq 0 ]; then rm -rf "$TMPDIR_T"; else echo "  (临时目录保留：$TMPDIR_T)"; fi
}
trap cleanup EXIT INT TERM

hr()   { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '  %s\n' "$*"; }
hdr()  { echo; hr; printf '%s\n' "$*"; }

FAILED=0
fail() { printf '  ✗ %s\n' "$*"; FAILED=1; }
pass() { printf '  ✓ %s\n' "$*"; }
skip() { printf '  - %s\n' "$*"; }

# ---------------------------------------------------------------- 工具函数

# start_server <port> [额外参数...]
start_server() {
    local port="$1"; shift
    : > "$TMPDIR_T/srv_${port}.log"
    "$BIN" --port="$port" "$@" >"$TMPDIR_T/srv_${port}.log" 2>&1 &
    SERVER_PID=$!

    local i
    for i in $(seq 1 50); do
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

# 从 JSON 里抠字段，抠不到返回 -1
#
# ★ 布尔必须归一成 1 / 0：
#   idle_probe.py 输出的是 json.dumps({"ok": True})，
#   Python 的 str(True) 是 "True"，脚本里 [ "$OK" = "1" ] 永远不成立。
#   这个 bug 在「功能没实现」时完全看不出来（那时候 ok 本来就是 false），
#   一实现就变成「测试全红但其实是好的」—— 比真 bug 更浪费时间。
jget() {  # $1=输出文本  $2=字段名
    printf '%s' "$1" | python3 -c '
import sys, json, re
s = sys.stdin.read()
m = re.search(r"\{.*\}", s, re.S)
if not m:
    print(-1); raise SystemExit
try:
    v = json.loads(m.group(0)).get(sys.argv[1], -1)
    if isinstance(v, bool):
        print(1 if v else 0)
    else:
        print(v)
except Exception:
    print(-1)
' "$2" 2>/dev/null || echo -1
}

fd_count() { ls "/proc/$1/fd" 2>/dev/null | wc -l | tr -d ' '; }

IDLE=2   # 统一用 2 秒超时：足够短让脚本快，又足够长避开 100ms 扫描精度

# ---------------------------------------------------------------- 0. 构建
hdr "[0/4] 准备"
if [ "$NO_BUILD" -eq 1 ]; then
    [ -x "$BIN" ] || { echo "  ✗ $BIN 不存在"; exit 2; }
    info "跳过编译（--no-build）"
else
    if ! cmake --build "$ROOT/build" --target bench_server -j >"$TMPDIR_T/build.log" 2>&1; then
        echo "  ✗ 编译失败："
        grep -E 'error' "$TMPDIR_T/build.log" | head -10 | sed 's/^/      /'
        exit 2
    fi
    info "编译完成"
fi
python3 -m py_compile "$PROBE" || { echo "  ✗ idle_probe.py 语法错误"; exit 2; }
info "服务端 = $BIN （model=$MODEL）"
info "探针   = $PROBE"
info "超时   = ${IDLE}s"

# ---- [0/4] 算法层单元测试：纯内存，不碰 socket，不睡觉 ----
# 分层原则：算法错了就不用往下测集成了，否则后面一堆 FAIL 都是同一个根因的噪音。
# 这里用手写 g++ 而不是 CMake target，与 02_blocking_queue.sh 保持一致：
# 单元测试要能脱离 cmake 单独跑（改一行、编一次、0.5 秒看结果）。
TIMER_UNIT="$ROOT/test/unit/timer_test.cpp"
if [ -f "$TIMER_UNIT" ]; then
    info "编译定时器单元测试..."
    if ! g++ -std=c++17 -Wall -Wextra -Werror \
            -I "$ROOT/lib" -I "$ROOT/src/timer" \
            "$TIMER_UNIT" -o "$TMPDIR_T/timer_unit" >"$TMPDIR_T/timer_unit_build.log" 2>&1; then
        echo "  ✗ 定时器单元测试编译失败："
        sed 's/^/      /' "$TMPDIR_T/timer_unit_build.log" | head -20
        exit 2
    fi
    if ! "$TMPDIR_T/timer_unit" >"$TMPDIR_T/timer_unit_run.log" 2>&1; then
        echo "  ✗ 定时器单元测试失败（算法层就不对，集成测试没有意义）："
        grep -E 'FAIL|pass=' "$TMPDIR_T/timer_unit_run.log" | sed 's/^/      /' | head -20
        exit 1
    fi
    pass "单元测试通过（$(grep -o 'pass=[0-9]*' "$TMPDIR_T/timer_unit_run.log" | head -1)）"
else
    info "跳过单元测试（$TIMER_UNIT 不存在）"
fi

# ---- 定时器接线层单测：IdleScanner（把 TimerQueue 粘到连接上那一层）----
# 和上面的 timer_test 分成两个二进制：算法层对了不代表接线层对了。
# 典型接线 bug：touch 变成「新增」而不是「刷新」、collect 后没从定时器摘除、
# timeout=0 没真正关闭。这些在 timer_test 里全都测不到。
SCANNER_UNIT="$ROOT/test/unit/idle_scanner_test.cpp"
if [ -f "$SCANNER_UNIT" ]; then
    info "编译 IdleScanner 单元测试..."
    if ! g++ -std=c++17 -Wall -Wextra -Werror \
            -I "$ROOT/lib" -I "$ROOT/src/timer" \
            "$SCANNER_UNIT" -o "$TMPDIR_T/scanner_unit" >"$TMPDIR_T/scanner_unit_build.log" 2>&1; then
        echo "  ✗ IdleScanner 单元测试编译失败："
        sed 's/^/      /' "$TMPDIR_T/scanner_unit_build.log" | head -20
        exit 2
    fi
    if ! "$TMPDIR_T/scanner_unit" >"$TMPDIR_T/scanner_unit_run.log" 2>&1; then
        echo "  ✗ IdleScanner 单元测试失败（接线层就不对，跑集成测试只会拿到一堆假现象）："
        grep -E 'FAIL|pass=' "$TMPDIR_T/scanner_unit_run.log" | sed 's/^/      /' | head -20
        exit 1
    fi
    pass "IdleScanner 通过（$(grep -o 'pass=[0-9]*' "$TMPDIR_T/scanner_unit_run.log" | head -1)）"
else
    info "跳过 IdleScanner 单元测试（$SCANNER_UNIT 不存在）"
fi

# 服务端是否真的实现了空闲超时（还没实现时给出明确提示，而不是一串莫名 FAIL）
#
# ★ 这个探针必须带 timeout：
#   未实现时 bench_server 会主动返回 2（很快）；
#   实现之后它会**真的起服务永久运行** —— 不带 timeout 就是脚本自己卡死在这里，
#   而且症状是一句输出都没有，比 FAIL 更难查。
#   timeout 触发（退出码 124）恰恰说明「服务起来了」= 功能已实现，属于通过。
#
# 判定表：
#   退出码 0/1/124  → 已经起来了 / 跑到被我们掐掉 → 功能存在
#   退出码 2 且日志有 not implemented → 明确未实现
#   其它            → 真正的启动错误
PROBE_RC=0
timeout 2 "$BIN" --port=1 --idle-timeout=1 >"$TMPDIR_T/probe_arg.log" 2>&1 || PROBE_RC=$?

if [ "$PROBE_RC" -eq 2 ] && grep -qi 'not implemented\|unknown arg' "$TMPDIR_T/probe_arg.log"; then
    echo
    echo "  ✗ 服务端还没有实现空闲超时（阶段 7.2）。"
    echo
    echo "  需要做两件事："
    echo
    echo "  ① 在 src/ 侧加接口（签名必须与 bench_server 的探测一致）："
    echo "       // server/tcp_server.hpp  —— TcpServerBase"
    echo "       virtual void set_idle_timeout(std::chrono::milliseconds ms) {"
    echo "           _idle_ms = ms;   // 0 = 关闭"
    echo "       }"
    echo "       protected: std::chrono::milliseconds _idle_ms{30000};"
    echo
    echo "  ② 在 Epoll_server 里真正用起来："
    echo "       · Connection 加 steady_clock::time_point _last_active（构造时 now()）"
    echo "       · recv>0 和 send 成功两处 touch()"
    echo "       · run_loop 的 dispatch 之后调 evict_idle()"
    echo "       · evict_idle：持锁收集过期 fd → 放锁 → 逐个 del_and_close"
    echo
    echo "  为什么必须主动报错：如果参数被接受但行为不存在，"
    echo "  本脚本测的正是这个行为，结果会是「假绿」——比编译失败更危险。"
    echo
    exit 2
elif [ "$PROBE_RC" -ne 0 ] && [ "$PROBE_RC" -ne 1 ] && [ "$PROBE_RC" -ne 124 ]; then
    echo "  ✗ bench_server 启动探针失败（退出码 $PROBE_RC）："
    sed 's/^/      /' "$TMPDIR_T/probe_arg.log"
    exit 2
fi
# 退出码 124 = 被 timeout 掐掉 = 它真的在服务，说明 --idle-timeout 已被接受
info "能力探测通过（退出码 $PROBE_RC$( [ "$PROBE_RC" -eq 124 ] && echo '，服务已在运行' )）"

# =============================================================================
# [1] 该杀的杀掉 —— epoll（及未来的主从 reactor）
# =============================================================================
hdr "[1/4] 该杀的杀掉：连上不说，服务端应主动断开"
info "超时=${IDLE}s。fail 的典型原因：扫描没生效 / 时间戳从不更新。"
echo

P=6501
for M in $MODEL; do
    if ! start_server "$P" --server="$M" --idle-timeout="$IDLE"; then
        echo "  ✗ 服务端启动失败（$M, 端口 $P）"
        sed 's/^/      /' "$TMPDIR_T/srv_${P}.log"
        exit 2
    fi
    FD_BASE=$(fd_count "$SERVER_PID")

    OUT="$(python3 "$PROBE" --port="$P" --mode=die --idle-timeout="$IDLE" --json 2>&1)"
    KIND=$(jget "$OUT" close_kind)
    ELAPSED=$(jget "$OUT" elapsed_s)
    OK=$(jget "$OUT" ok)

    # 等一下让服务端把 fd 收回去，再看回收情况
    sleep 0.5
    FD_AFTER=$(fd_count "$SERVER_PID")

    if [ "$OK" = "1" ]; then
        pass "$M：${ELAPSED}s 后断开（方式=$KIND）"
        if [ "$FD_AFTER" -gt 0 ] && [ "$FD_BASE" -gt 0 ]; then
            if [ "$FD_AFTER" -le $((FD_BASE + 1)) ]; then
                pass "$M：fd 已回收（$FD_BASE → $FD_AFTER）"
            else
                fail "$M：fd 未回收（$FD_BASE → $FD_AFTER）"
                info "断开动作发生了，但 fd 没被 close / 从 map 里摘掉。"
                info "epoll：del_and_close 里是否真的 erase 了 _conns？"
                info "检查顺序：先 _epoll.del(fd)，再持锁 _conns.erase(fd)；"
                info "缺了 del 会被 epoll 反复唤醒（关不掉），缺了 erase 则 map 一直长。"
            fi
        fi
    else
        case "$KIND" in
            still_alive)
                fail "$M：超过 ${ELAPSED}s 仍未被断开（超时未生效）"
                info "① --idle-timeout 是否真的传到了 evict 逻辑里（有没有被丢在半路）"
                info "② evict_idle() 是否在主循环里被调用（只定义了没调用很常见）"
                info "③ 时间戳初始值是否是 0 → now-0 恒大于阈值 → 会立刻杀，"
                info "   现象相反。若连接刚一建立就秒断，查这一条。"
                info "④ steady_clock 是否用对（若用 system_clock，NTP 校时会集体误杀）"
                ;;
            none|*)
                fail "$M：观察不到断开（kind=$KIND）"
                ;;
        esac
    fi
    stop_server
    P=$((P + 1))
done

# =============================================================================
# [2] 不该杀的留住 —— 刷新点是否覆盖 send 成功
# =============================================================================
hdr "[2/4] 不该杀的留住：每 0.5s 发一次，持续 4× 超时"
info "总时长 8s、超时 ${IDLE}s。若中途断，说明时间戳刷新点不全。"
info "典型缺陷：只在 recv 时刷，忘了 send 成功时也刷 —— "
info "          只推不读的连接（订阅/通知类业务）会被误杀。"
echo

P=6511
for M in $MODEL; do
    if ! start_server "$P" --server="$M" --idle-timeout="$IDLE"; then
        echo "  ✗ 服务端启动失败（$M, 端口 $P）"
        sed 's/^/      /' "$TMPDIR_T/srv_${P}.log"
        exit 2
    fi

    OUT="$(python3 "$PROBE" --port="$P" --mode=alive --idle-timeout="$IDLE" \
        --interval=0.5 --duration=8 --json 2>&1)"
    SENT=$(jget "$OUT" sent)
    RECV=$(jget "$OUT" recv)
    DROP=$(jget "$OUT" dropped)
    OK=$(jget "$OUT" ok)

    if [ "$OK" = "1" ]; then
        pass "$M：8s 内 ${RECV} 次往返全部成功，未被误杀"
    elif [ "$DROP" = "0" ] && [ "$RECV" = "0" ]; then
        fail "$M：一个响应都没收到（不是超时问题，是协议/服务端没起来）"
        info "先跑 05_tlv.sh 确认 echo 通路正常，再来测超时。"
    else
        fail "$M：中途被断开（sent=$SENT recv=$RECV dropped=$DROP）"
        info "超时判定应该用「上次活跃时间」而不是「上次扫描时间」，且"
        info "recv 有数据、send 成功这两处都要刷新时间戳。"
    fi
    stop_server
    P=$((P + 1))
done

# =============================================================================
# [3] 混合场景 —— 死的被清、活的零误伤
# =============================================================================
hdr "[3/4] 混合场景：5 条活连接 + 1 条死连接共处同一 epoll"
info "这是扫描式定时器最容易写错的地方：遍历 _conns 的同时 erase。"
info "期望：死的那条按时被清，5 条活的一次往返都不能丢，进程不崩。"
echo

P=6521
if ! start_server "$P" --server="$MODEL" --idle-timeout="$IDLE"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    sed 's/^/      /' "$TMPDIR_T/srv_${P}.log"
    exit 2
fi
FD0=$(fd_count "$SERVER_PID")

# 死连接：连上就静默，占着不动
python3 - "$P" <<'PY' >"$TMPDIR_T/dead.log" 2>&1 &
import socket, sys, time
socks = []
for _ in range(1):
    s = socket.socket(); s.connect(("127.0.0.1", int(sys.argv[1]))); socks.append(s)
time.sleep(60)
PY
DEAD_PID=$!

sleep 0.2
# 活连接：正常 echo 往返，持续 2.5× 超时（跨过死亡扫描点）
OUT="$(python3 "$PROBE" --port="$P" --mode=alive --idle-timeout="$IDLE" \
    --interval=0.5 --duration=5 --json 2>&1)"
RECV=$(jget "$OUT" recv)
DROP=$(jget "$OUT" dropped)
OK=$(jget "$OUT" ok)

kill -9 "$DEAD_PID" 2>/dev/null; wait "$DEAD_PID" 2>/dev/null
sleep 0.5
FD1=$(fd_count "$SERVER_PID")

if [ "$OK" = "1" ] && [ "$DROP" = "0" ] && [ "$RECV" -gt 0 ]; then
    pass "活连接在死连接被清的同时零丢包（${RECV} 次往返）"
else
    fail "活连接被牵连（recv=$RECV dropped=$DROP）"
    info "★ 扫描时遍历 _conns 又 erase，是未定义行为。必须两步："
    info "   ① 持锁只读遍历，把过期 fd 收进 vector<int>"
    info "   ② 放锁，再逐个 del_and_close(fd)"
fi

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    fail "服务端进程已退出 —— 很可能是遍历中 erase 导致崩溃"
    sed 's/^/      /' "$TMPDIR_T/srv_${P}.log" | tail -20
else
    pass "服务端进程存活"
    if [ "$FD1" -gt 0 ] && [ "$FD0" -gt 0 ]; then
        if [ "$FD1" -le $((FD0 + 5)) ]; then
            pass "fd 未泄漏（峰值 $FD0 → 收尾 $FD1）"
        else
            fail "fd 明显增长（$FD0 → $FD1）—— 死连接可能没被清掉"
        fi
    fi
fi
stop_server

# =============================================================================
# [4] 阈值边界 —— 区分「上次活跃时间」和「上次扫描时间」
# =============================================================================
hdr "[4/4] 边界：按 0.75× 超时的间隔持续发送，不应被断"
info "间隔必须【小于】超时值。若实现用「上次扫描时间」做基准，会有可观测漂移。"
info "这一条对主从 reactor 尤其重要：每个 Sub 的扫描相位不同，基准取错会变成随机误杀。"
echo

P=6531
EDGE_INT=$(python3 -c "print(round($IDLE * 0.75, 2))")
if ! start_server "$P" --server="$MODEL" --idle-timeout="$IDLE"; then
    echo "  ✗ 服务端启动失败（端口 $P）"
    sed 's/^/      /' "$TMPDIR_T/srv_${P}.log"
    exit 2
fi

OUT="$(python3 "$PROBE" --port="$P" --mode=alive --idle-timeout="$IDLE" \
    --interval="$EDGE_INT" --duration=8 --json 2>&1)"
RECV=$(jget "$OUT" recv)
DROP=$(jget "$OUT" dropped)
OK=$(jget "$OUT" ok)

if [ "$OK" = "1" ]; then
    pass "间隔 ${EDGE_INT}s 持续 8s 全部存活（${RECV} 次往返）"
else
    fail "间隔 ${EDGE_INT}s 却中途被断（recv=$RECV dropped=$DROP）"
    info "间隔已经明显小于超时值还被断，说明判定基准取错了。"
    info "正确：now - conn->last_active() > timeout"
    info "错误：now - last_scan_time      > timeout"
    info "错误：每次扫描就把所有时间戳重置一遍（看起来没删，实际永不超时）"
fi
stop_server

# =============================================================================
# 汇总
# =============================================================================
hdr "[汇总]"

if [ "$FAILED" -eq 0 ]; then
    echo "  ✓ PASS —— 空闲超时在 epoll（及主从 reactor 的 Sub 层）上成立"
    echo
    echo "  可以写进 README §五 证据表的数据："
    echo "    · die  ：${IDLE}s 后主动断开，fd 回收、_conns 缩回"
    echo "    · alive：4× 超时周期内零误杀"
    echo "    · 混合：死连接被清的同时，活连接零丢包，进程不崩"
    echo "    · 边界：0.75× 间隔持续发，全部存活"
    echo
    echo "  主从 reactor 落地时，这一章只需改一处："
    echo "    每个 Sub Reactor 线程自己扫自己的 _conns（单线程内无需加锁），"
    echo "    Main 线程只负责 accept 并分发，不碰定时器。"
    echo "    这正是 'one loop per thread' 比全局扫描优越的地方："
    echo "    没有跨线程 uint64 时间戳可见性问题，也没有共用锁的伪共享。"
    echo
    echo "  下一步（按依赖顺序）："
    echo "    1. TCP_NODELAY + SO_KEEPALIVE（阶段 7.1）"
    echo "       ★ KEEPALIVE 不能替代应用层超时：它探的是主机存活（默认 2 小时），"
    echo "         探不到「连上了但不发数据的应用」（slowloris）。"
    echo "    2. Ping/Pong 业务（tlv_defs.hpp）"
    echo "       ★ 空闲超时救不了「手机断网、FIN 没发出」——TCP 静默失败，"
    echo "         双方内核都认为连接正常。只有心跳往返能发现。"
    echo "       阈值配比：心跳 25s，空闲超时 >= 2.5×心跳 ≈ 75s。"
    echo "       chat 场景务必用大阈值（如 300s），用户不说话完全正常。"
    echo "    3. 小根堆 / 时间轮（有延迟任务需求时再做，扫描法在 500 连接足够）"
    hr
    exit 0
else
    echo "  ✗ FAIL —— 见上面 ✗ 标记"
    echo
    echo "  症状 → 根因 速查："
    echo "    still_alive           → evict_idle() 没被主循环调用"
    echo "    fd 未回收             → del_and_close 里缺 _conns.erase"
    echo "    一建连就断            → 时间戳初值是 0，now-0 恒大于阈值"
    echo "    alive 中途断          → 刷新点缺 send 成功那一处"
    echo "    混合场景进程退出      → 遍历 _conns 时 erase，迭代器失效"
    echo "    混合场景活连接被误杀  → 持锁删除导致另一半扫描看不到新时间戳"
    echo "    NTP 调整后才挂        → 用了 system_clock 而非 steady_clock"
    hr
    exit 1
fi