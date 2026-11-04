#include "mthread/pthread_server.hpp"
#include "mthread/thread_pool_server.hpp"
#include "server/tcp_server.hpp"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// 信号处理函数里只做异步信号安全的操作：置标志 + close(fd)
//注意这里不能自拔使用智能指针，不解决多线程/信号共享问题。要的是"能跨作用域拿到对象"，不是"转移所有权"，这两个需求不一样。
TcpServerBase* g_server = nullptr;

void on_signal(int) {
    if (g_server) {
        g_server->stop(); // _running=false 并 close(_listen_fd)，使阻塞中的 accept 返回
    }
}

} // namespace

int main(int argc, char** argv) {
    int port = 6003;

    // 参数：./socket_server [port]
    if (argc > 1) {
        port = std::atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            std::cerr << "invalid port: " << argv[1] << std::endl;
            return 1;
        }
    }

    ThreadPerConnServer server(port);//主要在这里切换

    // 业务回调：原样回写（echo），方便用 nc / telnet 手动验证
    server.set_message_callback([](int fd, const char* data, size_t len) {
        size_t sent = 0;
        while (sent < len) {
            // MSG_NOSIGNAL：对端已关闭时返回 EPIPE，而不是触发 SIGPIPE 杀掉进程
            ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                if (n < 0) perror("send");
                return;
            }
            sent += static_cast<size_t>(n);
        }
        std::cout << "[echo] fd=" << fd << " " << len << " bytes" << std::endl;
    });

    g_server = &server;

    std::signal(SIGINT, on_signal);  // Ctrl+C
    std::signal(SIGTERM, on_signal); // kill
    std::signal(SIGPIPE, SIG_IGN);   // 兜底：忽略写已关闭连接产生的 SIGPIPE

    std::cout << "listen on 0.0.0.0:" << port << std::endl;
    std::cout << "test with: nc 127.0.0.1 " << port << std::endl;

    server.start(); // 阻塞在此，直到收到 SIGINT/SIGTERM

    //g_server = nullptr;
    std::cout << "server stopped" << std::endl;
    return 0;
}


/**
    accept() 拿到 fd  →  submit( [this, fd]{ handle_connection(fd); } )
worker 拿到任务   →  在该连接上循环 recv / 回调 / close
*/
