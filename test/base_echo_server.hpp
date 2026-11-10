/**
 * ============================================================================
 *  base_echo_server.hpp  ——  串行基线（性能下限参照，仅 test/ 使用）
 * ============================================================================
 *
 * 【为什么需要这个模型】
 *   thread / pool / epoll 三个模型都比"最笨的实现"快，但快多少需要有个
 *   参照零点。这个类就是那个零点：完全阻塞、完全串行、同一时刻只服务 1 条连接。
 *
 *   它不是"又一个并发模型"，而是**对照组**。跑出来的数字用来回答：
 *     「上了线程/线程池/epoll，到底换来了什么？」
 *
 * 【语义（刻意选最笨的那个）】
 *   accept 一条 → 循环 recv 直到 EOF → 每段立即 echo → close → 再去 accept 下一条
 *
 *   所以：
 *     · 500 条空闲连接时，只有第 1 条在被服务，其余 499 条全在 accept 队列里排队
 *     · 线程数恒为 1（主线程自己干活，不派活）
 *     · 吞吐上限 = 单连接串行吞吐，加连接不会变快，只会变慢（排队）
 *
 * 【为什么不复用基类的 handle_client_read】
 *   基类那个只 recv 一次就返回，而且把数据 write 到 stdout（调试用）。
 *   这里需要「循环读到 EOF + 走 _msg_cb 回调」。
 *   另外注意：**不要和基类同名**，那是隐藏不是重载（见 thread_pool_server.hpp 的注释），
 *   所以这里叫 serve_one 而不是 handle_client_read。
 *
 * 【已知局限：退出时可能卡在 recv】
 *   stop() 只 close(listen_fd)，唤不醒已经阻塞在 recv 的那条连接。
 *   所以给 client fd 设了 SO_RCVTIMEO=200ms，超时后检查 _running 决定要不要退。
 *   代价：每条空闲连接每 200ms 多一次「返回 EAGAIN」的 syscall，可忽略。
 *   副作用：压测时 recv 超时不能当错误处理，必须 continue。
 * ============================================================================
 */
#pragma once

#include "server/tcp_server.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

class BaseEchoServer : public TcpServerBase {
public:
    explicit BaseEchoServer(int port) : TcpServerBase(port) {}

protected:
    void run_loop() override {
        while (_running) {
            sockaddr_in client_addr{};
            const int fd = accept_once(client_addr);
            if (fd < 0) {
                // EINTR 之外，只有当 stop() 关掉 listen_fd 后才该退出
                if (errno == EINTR) continue;
                if (!_running) return;
                // 其他错误（EMFILE 等）：串行基线不做退避状态机，直接重试。
                // 这是刻意的 —— 基线要足够笨，不能偷偷比别的模型多做事情。
                continue;
            }
            serve_one(fd);
        }
    }

private:
    // ⚠️ 刻意不叫 handle_client_read：那会隐藏基类的同名函数（隐藏 ≠ 重载）
    void serve_one(int fd) {
        // 200ms 读超时，只为让 stop() 能被感知；正常有数据时永不触发
        timeval tv{};
        tv.tv_sec  = 0;
        tv.tv_usec = 200 * 1000;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        char buf[4096];
        while (_running) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                // 阻塞 send 在 send_wbuf 里，客户端不读就会卡在这 ——
                // 这正是本基线要暴露的下限，不做任何规避
                if (_msg_cb) _msg_cb(fd, buf, static_cast<size_t>(n));
                continue;
            }
            if (n == 0) break;                       // 对端正常关闭
            if (errno == EINTR) continue;            // 信号打断，重试
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;                            // SO_RCVTIMEO 到期，回去看 _running
            }
            break;                                   // 真错误
        }
        ::close(fd);
    }
};