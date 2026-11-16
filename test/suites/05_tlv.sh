#!/usr/bin/env bash
# =============================================================================
# 05_tlv.sh
#
# 【测试点 5】TLV 协议链路端到端验证
#
# 被测链路
#   recv -> TLVDecoder::appendData -> tryDecode -> _frame_cb
#        -> Msg_Handle_Echo::pack -> send -> 客户端解包
#
# 为什么必须逐项验证，而不是"能回包就算通"
#   能回包只证明「单帧 + 一次 recv 收全 + seq 为 0」这一条最顺的路径。
#   真实的协议层还会遇到：
#     · 一次 recv 收到两帧         （粘包）  -> 内层 for(;;) 有没有循环
#     · 一帧跨两次 recv 才到齐     （拆包）  -> _buffer 有没有跨 recv 存活
#     · 响应里的 seq 必须等于请求   （回填）  -> setSeq 有没有被漏写
#     · flag 字段必须是确定的 0    （未初始化）-> 成员有没有 = 0
#   这 5 条各自对应 frame.hpp / thread_pool_server.hpp 里一个独立的缺陷面，
#   只测第 1 条会让后 4 个缺陷全部漏网。
#
# 用例
#   1  单帧往返       body 原样返回，cmd 变成 EchoResp
#   2  seq 回填       请求 seq=0x2A -> 响应 seq=0x2A（不是 0，不是随机）
#   3  flag 干净      响应 flag=0（未初始化时这里是栈垃圾）
#   4  粘包           两帧连发 -> 收到两个响应，共 2*(24+len) 字节
#   5  拆包           一帧分两次发（先 10B 头，再剩余）-> 正常回包
#   6  空 body        len=0 的帧也要能往返
#   7  坏校验和       服务端应丢帧不断连（可选，epoll/pool 行为一致）
#   8  坏 magic       服务端应关连接（可选）
#
# 用法
#   ./test/suites/05_tlv.sh                     # 默认测 pool 模型
#   ./test/suites/05_tlv.sh --server=epoll      # 换模型
#   ./test/suites/05_tlv.sh --no-build
#   ./test/suites/05_tlv.sh --strict            # 用例 7/8 失败也算 FAIL
#
# 退出码
#   0 = 全部 PASS   1 = 有 FAIL   2 = 环境/构建问题
# =============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/build/bench_server"
PROBE="$ROOT/test/tlv/tlv_probe.py"

MODEL="pool"
PORT="${PORT:-6230}"
NO_BUILD=0
STRICT=0
for arg in "$@"; do
    case "$arg" in
        --server=*) MODEL="${arg#--server=}" ;;
        --no-build) NO_BUILD=1 ;;
        --strict)   STRICT=1 ;;
        --port=*)   PORT="${arg#--port=}" ;;
        -h|--help)  sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)          echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

TMPDIR_T="$(mktemp -d)"
SERVER_PID=""

cleanup() {
    [ -n "$SERVER_PID" ] && { kill -9 "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; }
    rm -rf "$TMPDIR_T"
}
trap cleanup EXIT INT TERM

hr()   { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '  %s\n' "$*"; }
hdr()  { echo; hr; printf '%s\n' "$*"; }

FAILED=0
fail() { printf '  ✗ %s\n' "$*"; FAILED=1; }
pass() { printf '  ✓ %s\n' "$*"; }
warn() { printf '  ! %s\n' "$*"; }

# ---------------------------------------------------------------- 0. 构建
hdr "[0/3] 准备可执行文件（model=$MODEL port=$PORT）"
if [ "$NO_BUILD" -eq 1 ]; then
    [ -x "$BIN" ] || { echo "  ✗ $BIN 不存在，去掉 --no-build"; exit 2; }
    info "跳过编译（--no-build）"
else
    if ! cmake --build "$ROOT/build" --target bench_server -j >"$TMPDIR_T/build.log" 2>&1; then
        echo "  ✗ 编译失败，先把 frame.hpp 修好："
        grep -E 'error' "$TMPDIR_T/build.log" | head -10 | sed 's/^/      /'
        exit 2
    fi
    info "编译通过"
fi

# ------------------------------------------------- 1. 协议库自测（纯内存）
hdr "[1/3] TLV 协议库自测（不碰网络）"
if python3 "$PROBE"; then
    pass "协议参考实现自洽"
else
    fail "协议库自测失败（先修 tlv_probe.py，再谈服务端）"
    exit 1
fi

# ------------------------------------------------- 2. 启动服务端
hdr "[2/3] 启动 bench_server --server=$MODEL --port=$PORT"
: > "$TMPDIR_T/server.log"
"$BIN" --server="$MODEL" --port="$PORT" >"$TMPDIR_T/server.log" 2>&1 &
SERVER_PID=$!

READY=0
for _ in $(seq 1 60); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    if grep -q "model=" "$TMPDIR_T/server.log" 2>/dev/null; then READY=1; break; fi
    sleep 0.1
done
if [ "$READY" -ne 1 ]; then
    echo "  ✗ 服务端没起来。日志："
    sed 's/^/      /' "$TMPDIR_T/server.log"
    exit 2
fi
info "$(grep -m1 'model=' "$TMPDIR_T/server.log")"

# ------------------------------------------------- 3. 端到端用例
hdr "[3/3] 端到端用例"

python3 - "$PORT" "$PROBE" "$STRICT" <<'PY'
import importlib.util, socket, struct, sys, time

port, probe_path, strict = int(sys.argv[1]), sys.argv[2], sys.argv[3] == "1"

spec = importlib.util.spec_from_file_location("tlv_probe", probe_path)
probe = importlib.util.module_from_spec(spec); spec.loader.exec_module(probe)
pack, unpack_header, Cmd, MAGIC = probe.pack, probe.unpack_header, probe.Cmd, probe.MAGIC

fails = []
def chk(name, cond, extra=""):
    print(("  ✓ " if cond else "  ✗ ") + name + ("" if not extra else "   " + extra))
    if not cond: fails.append(name)

def conn(timeout=2.0):
    s = socket.create_connection(("127.0.0.1", port))
    s.settimeout(timeout)
    return s

def recv_exact(s, n):
    """读满 n 字节。超时抛 socket.timeout。"""
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("对端关闭，只收到 %d/%d 字节" % (len(buf), n))
        buf += chunk
    return buf

def recv_frame(s):
    """读一个完整帧，返回 (header, body)。"""
    hdr = recv_exact(s, probe.HEADER_SIZE)
    ln = unpack_header(hdr)["len"]
    body = recv_exact(s, ln) if ln else b""
    return unpack_header(hdr), body

# ---- 用例 1 / 2 / 3：单帧往返 + seq 回填 + flag 干净 ----
print("\n  [1] 单帧往返 + seq 回填 + flag 干净")
SEQ = 0x2A
BODY = b"hello"
try:
    s = conn()
    s.sendall(pack(Cmd.ECHO_REQ, BODY, seq=SEQ, flag=0))
    h, b = recv_frame(s)
    s.close()
    chk("长度 = 24+5", h["len"] == len(BODY), "len=%d" % h["len"])
    chk("cmd = EchoResp(0x%08X)" % Cmd.ECHO_RSP, h["cmd"] == Cmd.ECHO_RSP,
        "实际 0x%08X" % h["cmd"])
    chk("body 原样返回", b == BODY, repr(b))
    chk("seq 回填 = 0x%X" % SEQ, h["seq"] == SEQ,
        "实际 0x%X%s" % (h["seq"], "  <- setSeq 漏了" if h["seq"] == 0 else
                          "  <- _seq 未初始化" if h["seq"] > 0xFFFF else ""))
    chk("flag = 0", h["flag"] == 0,
        "实际 0x%X%s" % (h["flag"], "  <- _flag 未初始化！" if h["flag"] else ""))
except Exception as e:
    chk("单帧往返", False, "%s: %s" % (type(e).__name__, e))

# ---- 用例 4：粘包 ----
print("\n  [2] 粘包：两帧一次 send")
try:
    s = conn()
    p1 = pack(Cmd.ECHO_REQ, b"AAA", seq=1)
    p2 = pack(Cmd.ECHO_REQ, b"BBBB", seq=2)
    s.sendall(p1 + p2)
    h1, b1 = recv_frame(s)
    h2, b2 = recv_frame(s)
    s.close()
    chk("第一帧 body=AAA", b1 == b"AAA", repr(b1))
    chk("第一帧 seq=1", h1["seq"] == 1, "实际 %d" % h1["seq"])
    chk("第二帧 body=BBBB", b2 == b"BBBB", repr(b2))
    chk("第二帧 seq=2", h2["seq"] == 2, "实际 %d" % h2["seq"])
except Exception as e:
    chk("粘包", False, "%s: %s" % (type(e).__name__, e))

# ---- 用例 5：拆包（头先到 10 字节） ----
print("\n  [3] 拆包：一帧分两次发")
try:
    s = conn()
    p = pack(Cmd.ECHO_REQ, b"split-me", seq=0x55)
    s.sendall(p[:10]); time.sleep(0.15)
    s.sendall(p[10:])
    h, b = recv_frame(s)
    s.close()
    chk("body=split-me", b == b"split-me", repr(b))
    chk("seq=0x55", h["seq"] == 0x55, "实际 0x%X" % h["seq"])
    chk("半帧没丢（跨 recv 存活）", True)
except (socket.timeout, ConnectionError) as e:
    chk("拆包（半帧存活）", False, "%s: %s" % (type(e).__name__, e))
except Exception as e:
    chk("拆包", False, "%s: %s" % (type(e).__name__, e))

# ---- 用例 6：空 body ----
print("\n  [4] 空 body")
try:
    s = conn()
    s.sendall(pack(Cmd.ECHO_REQ, b"", seq=0x7))
    h, b = recv_frame(s)
    s.close()
    chk("len=0", h["len"] == 0, "len=%d" % h["len"])
    chk("body 为空", b == b"", repr(b))
    chk("seq=7", h["seq"] == 7, "实际 %d" % h["seq"])
except Exception as e:
    chk("空 body", False, "%s: %s" % (type(e).__name__, e))

# ---- 用例 7：坏校验和 -> 丢帧但不断连 ----
print("\n  [5] 坏校验和：丢该帧，连接仍可用")
try:
    s = conn()
    bad  = pack(Cmd.ECHO_REQ, b"corrupt", seq=9, corrupt_checksum=True)
    good = pack(Cmd.ECHO_REQ, b"after",   seq=10)
    s.sendall(bad + good)
    h, b = recv_frame(s)
    s.close()
    chk("坏帧被丢弃，后一帧仍被处理", b == b"after", repr(b))
    chk("后一帧 seq=10", h["seq"] == 10, "实际 %d" % h["seq"])
except (socket.timeout, ConnectionError) as e:
    msg = "  <- 坏帧把连接弄断了，应只丢该帧"
    if strict: chk("坏校验和不断连", False, "%s: %s" % (type(e).__name__, e) + msg)
    else:      print("  ! 坏校验和：连接被断开（C++ 侧 ERROR 分支直接断连）  " + msg)

# ---- 用例 8：坏 magic -> 关连接 ----
print("\n  [6] 坏 magic：应关连接")
try:
    s = conn()
    junk = b"\x00\x00\x00\x00" + pack(Cmd.ECHO_REQ, b"x")[4:]
    s.sendall(junk)
    try:
        data = s.recv(64)
        closed = (data == b"")            # 服务端关连接 -> 读到 EOF
    except (socket.timeout, ConnectionResetError):
        closed = True
    s.close()
    chk("magic 错 -> 连接被关闭", closed,
        "" if closed else "  <- 服务端没断开，可能把垃圾当帧在等 len")
except Exception as e:
    chk("坏 magic", False, "%s: %s" % (type(e).__name__, e))

print("\n%s" % ("全部通过" if not fails else "★ 失败 %d 项：%s" % (len(fails), ", ".join(fails))))
sys.exit(0 if not fails else 1)
PY

RC=$?
echo
hr
if [ "$RC" -eq 0 ]; then
    echo "结论：TLV 链路验证通过（model=$MODEL）"
else
    echo "结论：TLV 链路验证失败（model=$MODEL）"
    echo
    echo "服务端日志尾部："
    tail -20 "$TMPDIR_T/server.log" | sed 's/^/    /'
fi
hr
exit "$RC"