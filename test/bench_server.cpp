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
 *   ./test/bench_server --server=thread --port=6201
 *   ./test/bench_server --server=pool --port=6201 --workers=4 --queue-cap=1024
 *   ./test/bench_server --server=pool --port=6201 --workers=4 --verbose
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

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace {

TcpServerBase* g_server = nullptr;

// 信号上下文只做两件异步信号安全的事：置标志 + close(listen_fd)
void on_signal(int) {
    if (g_server) g_server->stop();
}

void usage(const char* prog) {
    std::cerr
        << "usage: " << prog << " [options]\n"
        << "  --server=thread|pool   并发模型（默认 thread）\n"
        << "  --port=N               监听端口（默认 6200）\n"
        << "  --workers=N            线程池 worker 数（默认 4，仅 pool 有效）\n"
        << "  --queue-cap=N          线程池待办队列容量（默认 1024，仅 pool 有效）\n"
        << "  --verbose              打印每条 echo（压测时非常慢，慎用）\n";
}

} // namespace

int main(int argc, char** argv) {
    int         port      = 6200;
    std::string model     = "thread";
    size_t      workers   = 4;
    size_t      queue_cap = 1024;
    bool        verbose   = false;

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
        else if (a == "--verbose")              { verbose = true; }
        else if (a == "-h" || a == "--help")    { usage(argv[0]); return 0; }
        else                                    { std::cerr << "unknown arg: " << a << "\n"; usage(argv[0]); return 2; }
    }

    if (port <= 0 || port > 65535) { std::cerr << "invalid port\n"; return 2; }
    if (workers == 0)              { std::cerr << "--workers must be > 0\n"; return 2; }

    // ---------- 按模型构造（unique_ptr 保证两种类型都能装）----------
    std::unique_ptr<TcpServerBase> server;
    if (model == "thread") {
        server = std::make_unique<ThreadPerConnServer>(port);
    } else if (model == "pool") {
        server = std::make_unique<ThreadPool_server>(port, workers, queue_cap);
    }else if (model == "epoll"){
        server = std::make_unique<EpollServer>(port);
    } else {
        std::cerr << "unknown server model: " << model << "\n";
        return 2;
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

    g_server = server.get();

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);

    // 这行是给 shell 脚本判断「端口已可用」用的，必须在 start() 之前打。
    // bind 是微秒级的，所以到脚本读到这行时基本已经 listen 了。
    std::cout << "bench_server: model=" << model
              << " port=" << port
              << " workers=" << (model == "pool" ? std::to_string(workers) : std::string("N/A"))
              << std::endl;
    std::cout.flush();

    server->start();   // 阻塞直到 SIGINT/SIGTERM

    std::cout << "bench_server: stopped" << std::endl;
    return 0;
}