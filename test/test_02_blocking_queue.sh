#!/usr/bin/env bash
# =============================================================================
# test_02_blocking_queue.sh
#
# 【测试点 2】lib/blocking_queue.hpp 的正确性与线程安全
#
# 被测代码
#   lib/blocking_queue.hpp   —— 有界阻塞队列（生产者/消费者）
#
# 测试内容（10 个用例，见 test/blocking_queue_test.cpp）
#   1  基本推拉              6  优雅关闭（close 后已排队的元素仍能取完）
#   2  FIFO 顺序             7  空队列 pop 阻塞 -> push 唤醒
#   3  move-only 类型        8  队满 push 阻塞 -> pop 唤醒（不丢数据）
#   4  MPMC 压力 10 万条     9  close 解除卡在 push 上的生产者（最关键）
#   5  close 语义           10  容量校验（cap==0 抛异常）
#
# 跑三遍，三种编译方式
#   (a) -O0 -Wall -Wextra   功能 + 编译警告
#   (b) -O2                 优化后行为是否一致（编译器更激进的假设容易暴露 UB）
#   (c) -fsanitize=thread   数据竞争检测
#
# 【关于 TSan 在这个容器里的坑】
#   TSan 第一次跑会直接 Segmentation fault：
#       FATAL: ThreadSanitizer: unexpected memory mapping 0x...
#   原因不是被测代码，而是容器把 /proc/sys/vm/mmap_rnd_bits 设成了 32，
#   地址随机化范围超出 TSan 运行时预期，崩在初始化阶段（还没执行被测代码）。
#   验证方法：用空 main 也能复现。
#   解法：setarch $(uname -m) -R <bin>  关掉 ASLR 再跑。
#   本脚本会自动检测并加这个前缀。
#
# 用法
#   ./test/test_02_blocking_queue.sh
#   ./test/test_02_blocking_queue.sh --no-tsan     # 跳过 TSan（更快）
#   ./test/test_02_blocking_queue.sh --only-tsan   # 只跑 TSan
#
# 退出码
#   0 = 全部 PASS   1 = 有 FAIL   2 = 环境/构建问题
# =============================================================================

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/test/blocking_queue_test.cpp"
OUT_DIR="$ROOT/build/test"

DO_O0=1
DO_O2=1
DO_TSAN=1

for arg in "$@"; do
    case "$arg" in
        --no-tsan)   DO_TSAN=0 ;;
        --only-tsan) DO_O0=0; DO_O2=0; DO_TSAN=1 ;;
        -h|--help)   sed -n '2,40p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "未知参数: $arg（-h 查看用法）" >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
#  前置检查
# ---------------------------------------------------------------------------
if [[ ! -f "$SRC" ]]; then
    echo "找不到 $SRC" >&2
    exit 2
fi
if [[ ! -f "$ROOT/lib/blocking_queue.hpp" ]]; then
    echo "找不到 lib/blocking_queue.hpp" >&2
    exit 2
fi
if ! command -v g++ >/dev/null 2>&1; then
    echo "找不到 g++" >&2
    exit 2
fi

mkdir -p "$OUT_DIR"

CXXFLAGS_COMMON=(-std=c++17 -I "$ROOT/lib" -pthread -g)

PASS_GROUPS=0
FAIL_GROUPS=0

# ---------------------------------------------------------------------------
#  跑一个构建变体
#    用法: run_variant <名字> <额外编译参数...>
# ---------------------------------------------------------------------------
run_variant() {
    local name="$1"; shift
    local bin="$OUT_DIR/bq_${name//[^a-zA-Z0-9]/_}"
    local log="$OUT_DIR/bq_${name//[^a-zA-Z0-9]/_}.build.log"

    echo
    echo "----------------------------------------------------------------"
    echo "  [$name] 编译中..."
    echo "----------------------------------------------------------------"

    if ! g++ "${CXXFLAGS_COMMON[@]}" "$@" "$SRC" -o "$bin" >"$log" 2>&1; then
        echo "  ❌ 编译失败（$name）："
        sed 's/^/     /' "$log" | head -40
        FAIL_GROUPS=$((FAIL_GROUPS + 1))
        return 1
    fi

    # 编译警告也要看：-Wall -Wextra 下应该零警告
    if [[ -s "$log" ]]; then
        echo "  ⚠️  编译警告："
        sed 's/^/     /' "$log" | head -20
    fi
    echo "  编译通过，运行中（timeout 180s）..."
    echo

    local run_cmd=("$bin")
    # TSan 需要关掉 ASLR，否则在 mmap_rnd_bits=32 的容器里起不来
    if [[ "$name" == *tsan* ]] && command -v setarch >/dev/null 2>&1; then
        run_cmd=(setarch "$(uname -m)" -R "$bin")
    fi

    timeout 180 "${run_cmd[@]}"
    local rc=$?

    if [[ $rc -eq 124 ]]; then
        echo "  ❌ [$name] 超时 180s —— 大概率是死锁" >&2
        FAIL_GROUPS=$((FAIL_GROUPS + 1))
        return 1
    elif [[ $rc -ne 0 ]]; then
        echo "  ❌ [$name] 有 FAIL 或崩溃（exit=$rc）" >&2
        FAIL_GROUPS=$((FAIL_GROUPS + 1))
        return 1
    fi

    echo "  ✅ [$name] 全部通过"
    PASS_GROUPS=$((PASS_GROUPS + 1))
    return 0
}

echo "================================================================"
echo "  BlockingQueue 测试"
echo "  源码: $SRC"
echo "  输出: $OUT_DIR"
echo "================================================================"

# ---------------------------------------------------------------------------
#  (a) -O0：功能 + 编译警告
# ---------------------------------------------------------------------------
if [[ $DO_O0 -eq 1 ]]; then
    run_variant "O0-wall" -O0 -Wall -Wextra
fi

# ---------------------------------------------------------------------------
#  (b) -O2：优化后行为一致性
#      编译器在 -O2 下会做更激进的假设，若代码有 UB（比如未初始化读、
#      严格别名违规、无锁读写共享变量），这里比 -O0 更容易暴露。
# ---------------------------------------------------------------------------
if [[ $DO_O2 -eq 1 ]]; then
    run_variant "O2" -O2 -Wall -Wextra
fi

# ---------------------------------------------------------------------------
#  (c) TSan：数据竞争
# ---------------------------------------------------------------------------
if [[ $DO_TSAN -eq 1 ]]; then
    if ! echo 'int main(){return 0;}' | g++ -fsanitize=thread -x c++ - -o "$OUT_DIR/tsan_probe" >/dev/null 2>&1; then
        echo
        echo "  ⚠️  编译器不支持 -fsanitize=thread，跳过 TSan" >&2
    else
        run_variant "tsan" -O1 -fsanitize=thread
    fi
    rm -f "$OUT_DIR/tsan_probe"
fi

# ---------------------------------------------------------------------------
#  汇总
# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "  汇总：通过 $PASS_GROUPS 组，失败 $FAIL_GROUPS 组"
if [[ -f /proc/sys/vm/mmap_rnd_bits ]]; then
    echo "  (本机 mmap_rnd_bits=$(cat /proc/sys/vm/mmap_rnd_bits)，"
    echo "   TSan 需要 setarch -R 前缀，脚本已自动处理)"
fi
echo "================================================================"

if [[ $FAIL_GROUPS -eq 0 ]]; then
    echo "✅ RESULT: PASS"
    exit 0
else
    echo "❌ RESULT: FAIL"
    exit 1
fi