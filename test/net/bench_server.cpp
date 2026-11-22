/**
 * ============================================================================
 *  bench_server.cpp  ——  压测专用服务端入口（仅用于 test/，不进 src/main.cpp）
 * ============================================================================
 *
 * 【为什么要单独一个入口】
 *   src/main.cpp 是你手动验证用的，里面直接写死了 ThreadPerConnServer。
 *   压测需要「同一个脚本对比两种模型」，所以这里用 --server= 运行时选择。
 *   这样 src/main.cpp 一行都不用改，测试代码全部隔离在 test/ 目录。
 *
 * 【用法】
 *   ./build/bench_server --server=base --port=6201                      # 串行基线
 *   ./build/bench_server --server=thread --port=6201
 *   ./build/bench_server --server=pool --port=6201 --workers=4 --queue-cap=1024
 *   ./build/bench_server --server=epoll --port=6201
 *   ./build/bench_server --server=pool --port=6201 --workers=4 --verbose
 *
 * 【为什么默认不打印每条消息】
 *   原来的 echo 回调里有 std::cout << "[echo] fd=..."。
 *   压测时每秒几万条消息 = 几万次 write 系统调用 + 终端渲染，
 *   测出来的 QPS 全是 stdout 的瓶颈，跟网络模型毫无关系。
 *   所以 bench 默认静默，--verbose 才打开。
 *
 * 【退出码】
 *   0 = 正常（收到 SIGINT/SIGTERM 后干净退出）
 *   2 = 参数错误 / 未知模型
 * ============================================================================
 */

#include "server/tcp_server.hpp"
#include "mthread/pthread_server.hpp"
#include "mthread/thread_pool_server.hpp"
#include "mthread/Epoll_server.hpp"
#include "mthread/OLPT_server.hpp"
#include "base_echo_server.hpp"   // 串行基线（test-only，不进 src/）

#include <csignal>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <sys/socket.h>
#include <unistd.h>
#include "protocol/tlv_defs.hpp"

namespace {

TcpServerBase* g_server = nullptr;

// ---------------------------------------------------------------- 能力探测
// 阶段 7.2 之前 TcpServerBase 没有 set_idle_timeout，
// 直接调用会编译失败，那样连 --help 都跑不了。
// 用 SFINAE 探测「成员是否存在」，未实现时走 else 分支大声报错。
namespace detail {

template <class T, class = void>
struct has_set_idle_timeout : std::false_type {};

template <class T>
struct has_set_idle_timeout<T, std::void_t<decltype(
    std::declval<T&>().set_idle_timeout(std::chrono::milliseconds{0}))
>> : std::true_type {};

// 无法用 if constexpr 在非模板上下文里保护「不存在的成员」：
// 被丢弃的分支仍然要求语义正确。所以真正调用必须放在函数模板里，
// 由重载决议在编译期挑一个。
template <class T>
bool try_set_idle_timeout(T& srv, std::chrono::milliseconds ms) {
    if constexpr (has_set_idle_timeout<T>::value) {
        srv.set_idle_timeout(ms);
        return true;
    } else {
        (void)srv; (void)ms;
        return false;
    }
}

} // namespace detail

// 信号上下文只做两件异步信号安全的事：置标志 + close(listen_fd)
void on_signal(int) {
    if (g_server) g_server->stop();
}

void usage(const char* prog) {
    std::cerr
        << "usage: " << prog << " [options]\n"
        << "  --server=base|thread|pool|epoll|olpt  并发模型（默认 thread）\n"
        << "                         base = 串行基线（阻塞到底，同时只服务 1 条连接）\n"
        << "  --port=N               监听端口（默认 6200）\n"
        << "  --workers=N            业务线程数（默认 4；pool / epoll 有效）\n"
        << "  --queue-cap=N          待办队列容量（默认 1024；pool / epoll 有效）\n"
        << "  --idle-timeout=N       空闲超时秒数（默认 30，0 = 关闭；仅 epoll/reactor 有效）\n"
        << "  --biz-delay=N          业务回调人为阻塞 N 毫秒（默认 0）\n"
        << "                         ← 用来对比「业务阻塞时不同并发模型的反应」\n"
        << "                         另外 body 以 \"SLOW:<ms>:\" 开头可单条覆盖\n"
        << "  --pool-per-sub=N       olpt 专属：每个从 reactor 挂 N 个业务线程（默认 1）\n"
        << "                         0 = 不挂，业务直接在从 reactor 的 I/O 线程里跑（对照组）\n"
        << "  --verbose              打印每条 echo（压测时非常慢，慎用）\n";
}

} // namespace

int main(int argc, char** argv) {
    int         port      = 6200;
    std::string model     = "olpt";
    size_t      workers   = 4;
    size_t      queue_cap = 1024;
    bool        verbose   = false;

    // 空闲超时（阶段 7.2）。0 = 关闭。
    // 默认 30s 而不是 5s：既有测试用 --hold=6 / --hold=8，
    // 阈值取太小会把这些测试弄成假红（报出来像 accept 的 bug）。
    size_t      idle_s    = 30;
    bool        idle_given = false;   // 显式传过 --idle-timeout（含 0）

    // 业务回调人为阻塞（毫秒）。0 = 不阻塞。
    // 这是「线程阻塞对比实验」的唯一开关：
    //   没有它，业务全是纯 echo（微秒级），epoll 和 olpt 的差异被淹没在噪声里。
    size_t      biz_delay_ms = 0;

    // olpt 专属：每个从 Reactor 挂几个业务线程。
    //   1 = 标准形态（业务不阻塞 I/O）
    //   0 = 不挂池子，业务直接在从 Reactor 的 I/O 线程里跑（对照组，E4 会变 2.18s）
    size_t      pool_per_sub = 1;

    // 统一的 --key=value 解析
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto value_of = [&](const std::string& key) -> std::string {
            // 支持 --key=value 和 --key value 两种写法
            if (a.rfind(key + "=", 0) == 0) return a.substr(key.size() + 1);
            if (a == key) {
                if (i + 1 >= argc) { std::cerr << key << " needs a value\n"; std::exit(2); }
                return argv[++i];
            }
            return {};
        };

        if (a.rfind("--server", 0) == 0)        { auto v = value_of("--server");  if (!v.empty()) model = v; }
        else if (a.rfind("--port", 0) == 0)     { auto v = value_of("--port");    if (!v.empty()) port = std::atoi(v.c_str()); }
        else if (a.rfind("--workers", 0) == 0)  { auto v = value_of("--workers"); if (!v.empty()) workers = std::strtoul(v.c_str(), nullptr, 10); }
        else if (a.rfind("--queue-cap", 0) == 0){ auto v = value_of("--queue-cap"); if (!v.empty()) queue_cap = std::strtoul(v.c_str(), nullptr, 10); }
        else if (a.rfind("--idle-timeout", 0) == 0){
            auto v = value_of("--idle-timeout");
            if (!v.empty()) { idle_s = std::strtoul(v.c_str(), nullptr, 10); idle_given = true; }
        }
        else if (a.rfind("--biz-delay", 0) == 0) {
            auto v = value_of("--biz-delay");
            if (!v.empty()) biz_delay_ms = std::strtoul(v.c_str(), nullptr, 10);
        }
        else if (a.rfind("--pool-per-sub", 0) == 0) {
            auto v = value_of("--pool-per-sub");
            if (!v.empty()) pool_per_sub = std::strtoul(v.c_str(), nullptr, 10);
        }
        else if (a == "--verbose")              { verbose = true; }
        else if (a == "-h" || a == "--help")    { usage(argv[0]); return 0; }
        else                                    { std::cerr << "unknown arg: " << a << "\n"; usage(argv[0]); return 2; }
    }

    if (port <= 0 || port > 65535) { std::cerr << "invalid port\n"; return 2; }
    if (workers == 0)              { std::cerr << "--workers must be > 0\n"; return 2; }

    // ---------- 按模型构造（unique_ptr 保证多种类型都能装）----------
    std::unique_ptr<TcpServerBase> server;
    if (model == "base") {
        server = std::make_unique<BaseEchoServer>(port);
    } else if (model == "thread") {
        server = std::make_unique<ThreadPerConnServer>(port);
    } else if (model == "pool") {
        server = std::make_unique<ThreadPool_server>(port, workers, queue_cap);
    } else if (model == "epoll"){
        // epoll 模型：reactor 只做 I/O，业务在 workers 个线程里跑
        server = std::make_unique<EpollServer>(port, workers, queue_cap);
    } else if (model == "olpt"){
        // 主从 reactor：1 个 acceptor + workers 个从 reactor，
        // 每个从 reactor 再挂 pool_per_sub 个业务线程（0 = 不挂，业务在 I/O 线程里跑）
        server = std::make_unique<OLPT_Server>(port, workers, pool_per_sub, queue_cap);
    } else {
        std::cerr << "unknown server model: " << model << "\n";
        return 2;
    }

    // ---------- 接入空闲超时 ----------
    // 这里用「能力探测」而不是硬编码调用：阶段 7.2 还没实现时，
    // 编译不能挂（否则整套测试都跑不了），但显式传了 --idle-timeout
    // 就必须【大声报错并返回非 0】——
    // 否则 06_timer.sh 会拿到一个「参数被接受、行为却不存在」的服务端，
    // 而它测的恰恰就是这个行为，结果就是假绿。
    //
    // 期望的接口（由 src/ 侧实现，签名不匹配时不报编译错）：
    //     void TcpServerBase::set_idle_timeout(std::chrono::milliseconds ms);
    //                                              ^ 毫秒，0 = 关闭
    if (idle_given) {
        if (detail::try_set_idle_timeout(*server, std::chrono::milliseconds(idle_s * 1000))) {
            if (verbose) {
                std::cout << "[idle] timeout = " << idle_s << "s"
                          << (idle_s == 0 ? " (disabled)" : "") << std::endl;
            }
        } else {
            std::cerr
                << "--idle-timeout is not implemented yet.\n"
                << "  TcpServerBase 还没有 set_idle_timeout(std::chrono::milliseconds)。\n"
                << "  这正是阶段 7.2 要做的事，实现前 test/suites/06_timer.sh 应当失败。\n";
            return 2;
        }
    }

    // ---------- echo 回调 ----------
    // 回调里绝对不要直接调 ::send：
    //   1) 线程模型下会阻塞当前工作线程；
    //   2) epoll 模型下会绕过 wbuf，慢客户端把 reactor 拖死，
    //      send_wbuf 里的 EAGAIN/EPOLLOUT 分支永远执行不到。
    // 统一走 TcpServerBase::send() 门面，由派生类自己决定「直发还是排队」。
    // 注意不能写 server.get()->send_wbuf(...)：静态类型是 TcpServerBase*，
    // 而 send_wbuf 在那里还是 protected，编译不过。
    TcpServerBase* srv = server.get();
    server->set_message_callback([verbose, srv](int fd, const char* data, size_t len) {
        srv->send(fd, data, len);   // 重试 / 缓冲 / 关闭全在 send_wbuf 内
        if (verbose) {
            std::cout << "[echo] fd=" << fd << " " << len << " bytes" << std::endl;
        }
    });

    //----------  TLV回调----------------
    server->set_frame_callback([verbose,srv,biz_delay_ms](int fd,const FrameHeader& hdr,const std::string& body){
        //1.先过滤cmd：不是EchoReq就丢弃
        if(hdr.cmd != static_cast<uint32_t>(Msg_frame::Cmd::EchoReq)){
            if (verbose) {
                std::cout << "[drop] fd=" << fd
                          << " cmd=0x" << std::hex << hdr.cmd << std::dec
                          << std::endl;
            }
            return;
        }

        //1.5 人为阻塞：这是「线程阻塞对比实验」的核心注入点。
        //    两种模型的差别全在「这行 sleep 跑在哪个线程里」：
        //      epoll → 线程池的 worker 线程（reactor 不受影响，仍在 recv/accept）
        //      olpt  → 从 reactor 自己的线程（该 shard 的 I/O 也一起停摆）
        {
            size_t ms = biz_delay_ms;
            // body 形如 "SLOW:800:xxx" → 单条覆盖成 800ms
            if (body.rfind("SLOW:", 0) == 0) {
                ms = std::strtoul(body.c_str() + 5, nullptr, 10);
            }
            if (ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            }
        }

        //2.构造响应：body原样返回，is_resp = true 让 getCmd（）返回EchoResp
        Msg_Handle_Echo resp(body,/*is_resp = */true);
        resp.setSeq(hdr.seq);
        std::string pkt = resp.pack();

        //3.回包（pool 里 worker 独占连接，可直接 send）
        srv->send(fd, pkt.data(), pkt.size());

        if (verbose) {
            std::cout << "[echo] fd=" << fd << " body=" << body
                      << " (" << pkt.size() << " bytes)" << std::endl;
        }
    });


    g_server = server.get();

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);

    // 这行是给 shell 脚本判断「端口已可用」用的，必须在 start() 之前打。
    // bind 是微秒级的，所以到脚本读到这行时基本已经 listen 了。
    std::cout << "bench_server: model=" << model
              << " port=" << port
              << " workers=" << ((model == "pool" || model == "epoll" || model == "olpt") ? std::to_string(workers) : std::string("N/A"))
              << (model == "olpt" ? (" pool_per_sub=" + std::to_string(pool_per_sub)) : std::string())
              << std::endl;
    std::cout.flush();

    server->start();   // 阻塞直到 SIGINT/SIGTERM

    std::cout << "bench_server: stopped" << std::endl;
    return 0;
}