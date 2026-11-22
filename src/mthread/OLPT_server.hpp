#pragma once

/**
 * ============================================================================
 *  OLPT_server.hpp —— One Loop Per Thread（主从 Reactor）
 * ============================================================================
 *
 *  g++ -std=c++17 -fsyntax-only -I./lib -I./src src/mthread/OLPT_server.hpp
 *
 * ---------------------------------------------------------------------------
 *  【拓扑】
 *
 *        主 Reactor（1 线程）                从 Reactor（N 线程）
 *     ┌──────────────────────┐          ┌──────────────────────┐
 *     │  epoll_wait          │          │  epoll_wait          │
 *     │  只听 listen_fd      │  accept  │  听自己分到的 cfd     │
 *     │  ↓ 轮询分发 fd % N   │ ───────► │  ↓ recv/decode/业务   │
 *     └──────────────────────┘          └──────────────────────┘
 *                                              ×N，各自独占
 *                                        _conns / wbuf / epoll
 *
 * ---------------------------------------------------------------------------
 *  【三条铁律】
 *
 *  1. 每个 fd 只属于一个从 Reactor。谁 accept 谁不碰业务，谁拥有 fd 谁处理到底。
 *  2. 跨线程不碰别人的状态。主 Reactor 想往从 Reactor 里塞 fd，只能
 *     走 run_in_loop()（入队 + eventfd 唤醒），不能直接 epoll_ctl / _conns.insert。
 *     否则从线程正在 epoll_wait，你从外面改它的表 —— 那就是数据竞争。
 *  3. 从 Reactor 之间零共享。_conns、wbuf、Epoll 全部线程私有，一把锁都不用。
 *     这就是 OLPT 相对「单 reactor + 全局 mutex」的全部价值。
 *
 * ---------------------------------------------------------------------------
 *  【和 Epoll_server.hpp 的区别】
 *
 *  Epoll_server：1 reactor 只做 I/O + 线程池跑业务  → 适合业务会阻塞
 *  OLPT_server ：N reactor 自己就是业务线程          → 适合业务是纯计算/短任务
 *
 *  两者可以叠加：每个从 Reactor 再挂一个 IoThreadPool，就是 nginx 的最终形态。
 *  OLPT + 线程池：每个从 Reactor 再挂一个 IoThreadPool，就是 nginx 最终形态
 *  SO_REUSEPORT：彻底去掉主 Reactor，N 个进程/线程各自 listen，内核帮你分流（阶段 4）
 *  负载不均：_next % N 是轮询，慢连接多的 shard 会积压 → 换 least-connection
 *  ============================================================================
 */

#include "server/tcp_server.hpp"
#include "Connection.hpp"
#include "epoll.hpp"
#include "Iothread_pool.hpp"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include "mthread/thread_pool_server.hpp"


// ============================================================================
//  SubReactor —— 一个线程 + 一个 Epoll + 一张连接表
// ============================================================================
class SubReactor{
public:
    using Task = std::function<void()>;

    static constexpr int    kMaxEvents = 1024;
    static constexpr int    kWaitMs    = 100;
    static constexpr size_t kReadBuf   = 4096;
    // LT 模式下的读配额：一次事件最多搬这么多字节就让路，
    // 没搬完的靠下一次 EPOLLIN 继续（LT 会一直报，安全）。
    static constexpr size_t kReadQuota = 64 * 1024;

    explicit SubReactor(size_t id,
                        TcpServerBase::FrameCallback frame_cb,
                        std::function<void(size_t,int)> on_open,
                        std::function<void(size_t,int)> on_close,
                        size_t pool_threads = 1,
                        size_t queue_cap = 1024)
        : _id(id)
        , _frame_cb(std::move(frame_cb))
        , _on_open(std::move(on_open))
        , _on_close(std::move(on_close))
    {
        if(pool_threads > 0) _pool = std::make_unique<IoThreadPool>(pool_threads,queue_cap);
        _epoll = std::make_unique<Epoll>(EPOLL_CLOEXEC, kMaxEvents);

        // eventfd：别的线程往我这里塞任务时，用它把我从 epoll_wait 里踢醒。
        // 没有它，被塞进来的 fd 最多要等 kWaitMs 才被处理。
        _wake_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (_wake_fd < 0){
            throw std::runtime_error("eventfd: " + std::string(std::strerror(errno)));
        }
        _epoll->add(_wake_fd, EPOLLIN);
    }

    ~SubReactor(){
        stop();
        if(_pool) _pool->shutdown();
        if (_wake_fd >= 0){ ::close(_wake_fd); _wake_fd = -1; }
    }

    SubReactor(const SubReactor&) = delete;
    SubReactor& operator=(const SubReactor&) = delete;

    size_t id() const noexcept { return _id; }

    void start(){
        _th = std::thread([this]{ loop(); });
    }

    void stop(){
        _running = false;
        wakeup();
        if (_th.joinable()) _th.join();
    }

    // ------------------------------------------------------------------
    //  唯一的跨线程入口
    // ------------------------------------------------------------------
    /**
     * 把任务丢到本 Reactor 的线程里执行。
     * 已经是本线程 → 直接跑（省一次唤醒，~2μs）
     * 否则         → 入队 + eventfd 唤醒
     */
    void run_in_loop(Task t){
        if (is_current_thread()){
            t();
            return;
        }
        {
            std::lock_guard<std::mutex> lk(_q_mtx);
            _q.push_back(std::move(t));
        }
        wakeup();
    }

    bool is_current_thread() const noexcept {
        // 用 thread_local 比较而不是 thread::id：无锁、无系统调用
        return tls_current == this;
    }

    // ------------------------------------------------------------------
    //  连接生命周期（只会在本线程执行）
    // ------------------------------------------------------------------
    /** 接管一个刚从 accept 出来的 fd */
    void adopt(int cfd){
        if (_running == false || cfd < 0){ ::close(cfd); return; }

        // fd 已经是 SOCK_NONBLOCK | SOCK_CLOEXEC（accept4 保证）
        auto conn = std::make_shared<Connection>(cfd);

        if (!_epoll->add(cfd, EPOLLIN)){
            // 交给 Connection 析构去 close，我们不碰裸 close
            return;
        }
        _conns.emplace(cfd, std::move(conn));

        if (_on_open) _on_open(_id, cfd);
    }

    /** 本线程内的发送（业务回调里通常会走这条） */
    void send(int fd, const char* data, size_t n){
        if (fd < 0 || data == nullptr || n == 0) return;
        auto it = _conns.find(fd);
        if (it == _conns.end()) return;
        flush(it->second, data, n);
    }

private:
    // ------------------------------------------------------------------
    //  线程主循环
    // ------------------------------------------------------------------
    void loop(){
        tls_current = this;                 // 让 run_in_loop 能识别"这是我自己的线程"

        std::vector<struct epoll_event> evs(kMaxEvents);

        while (_running){
            int n = _epoll->wait(evs, kWaitMs);
            if (n < 0) break;

            for (int i = 0; i < n; ++i){
                const int      fd = evs[i].data.fd;
                const uint32_t ev = evs[i].events;

                // ① 唤醒 fd 必须最先判断：它不在 _conns 里，
                //    掉进下面的分支只会打一堆无意义的错误日志
                if (fd == _wake_fd){ handle_wakeup(); continue; }

                // ② 出错优先：ERR 常与 IN 同时置位，放在末尾会被吞掉
                if (ev & EPOLLERR){ close_conn(fd); continue; }

                // ③ HUP = 对端关闭，但内核里可能还有没读完的数据，先榨干
                if (ev & EPOLLHUP){
                    if (ev & EPOLLIN) handle_read(fd);
                    else              close_conn(fd);
                    continue;
                }

                // ④ IN / OUT 互不排斥，同一 fd 可能两个都有
                if (ev & EPOLLIN)  handle_read(fd);
                if (ev & EPOLLOUT) handle_write(fd);
                // 注意：handle_read 里可能已经把 fd 关了，
                //       那时 handle_write 的 find 会失败并安全返回
            }

            // 收尾：把本轮积压的跨线程任务跑掉
            do_pending();
        }

        // 退出：把所有连接摘干净（erase 触发 ~Connection → ::close）
        for (auto& kv : _conns) _epoll->del(kv.first);
        _conns.clear();
        tls_current = nullptr;
    }

    // ------------------------------------------------------------------
    //  读 / 分帧
    // ------------------------------------------------------------------
    void handle_read(int fd){
        auto it = _conns.find(fd);
        if (it == _conns.end()) return;
        std::shared_ptr<Connection> conn = it->second;

        char   buf[kReadBuf];
        size_t budget = kReadQuota;

        while (budget > 0){
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);

            if (n > 0){
                budget = (static_cast<size_t>(n) >= budget)
                       ? 0 : budget - static_cast<size_t>(n);

                conn->decoder().appendData(buf, static_cast<size_t>(n));

                FrameHeader hdr;
                std::string body;
                for (;;){
                    auto st = conn->decoder().tryDecode(hdr, body);

                    // 流已不可信 → 关连接，立刻 return（不能再 tryDecode）
                    if (st == TLVDecoder::Status::BAD_ERROR){ close_conn(fd); return; }
                    // 校验和错：只丢这一帧，流继续可用
                    if (st == TLVDecoder::Status::ERROR) continue;
                    // 半帧：等下一次 EPOLLIN
                    if (st == TLVDecoder::Status::NEED_MORE) break;

                    // 完整一帧 → 业务（挂池子就走池子，不挂就在本线程跑）
                    if (st == TLVDecoder::Status::OK){
                        if (!dispatch_frame(conn, fd, hdr, body)) return;  // false = 池满，连接已拆
                        // 业务回调 / 背压处理里可能自己关了连接
                        if (_conns.find(fd) == _conns.end()) return;
                    }
                }
            } else if (n == 0){
                close_conn(fd);     // 对端 FIN
                return;
            } else {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                close_conn(fd);
                return;
            }
        }
        // 配额用完 → 剩下的留给下一次 EPOLLIN（LT 保证还会报）
    }

    /**
     * ★ 把一帧交给业务线程池
     *
     * 不挂池子（_pool == nullptr）时退化成原行为：就在 I/O 线程里跑，
     * 慢业务会把该 shard 的 epoll_wait 一起卡住（E4 的 2.18s 就是这么来的）。
     *
     * @return false 表示待办队列满（背压），此时连接已被拆掉
     */
    bool dispatch_frame(const std::shared_ptr<Connection>& conn,
                        int fd, const FrameHeader& hdr, const std::string& body){
        if (!_frame_cb) return true;

        // ── 没挂池子：I/O 线程自己跑（对照组行为）──
        if (!_pool){
            _frame_cb(fd, hdr, body);
            return true;
        }

        // ── 挂池子：投递到本 shard 的 worker ──
        //
        // 捕获 shared_ptr 而不是裸 fd：
        //   持池期间 Connection 不会被析构 ⇒ fd 不会被 close
        //   ⇒ 内核不可能把这个号发给新连接 ⇒ 不存在「写到别人连接上」的 fd 复用灾难
        auto task = [this, fd, conn, hdr, body]{
            if (conn->closed()) return;      // 期间被 reactor 关了，别白跑
            _frame_cb(fd, hdr, body);
        };

        if (_pool->submit(std::move(task))) return true;

        // ★ 背压：这帧已经从 decoder 里消费掉了，丢掉它 = 流损坏，
        //   客户端会收到半截/错位的包。宁可断开连接，也不能发出错误数据。
        //   （阶段 3 要做的事：改成暂停 EPOLLIN，队列空了再恢复，而不是断连）
        close_conn(fd);
        return false;
    }

    // ------------------------------------------------------------------
    //  写 / wbuf
    // ------------------------------------------------------------------
    void flush(const std::shared_ptr<Connection>& conn, const void* data, size_t n){
        if (!conn || conn->closed()) return;
        const int fd = conn->fd();
        if (fd < 0) return;

        // ★ fd 复用防护（挂上业务池之后才致命，之前 flush 只在属主线程被调用）
        //
        //   worker 手里的 shared_ptr 会延长 Connection 的寿命，所以 reactor 那边
        //   close_conn 摘掉连接、::close(fd) 之后，这个对象的 closed() 仍然是 false。
        //   而 Linux 的 fd 号是最低可用优先 —— 这个号极可能已经被 accept 给了新连接。
        //   此时若还往 fd 里 send，数据就直接串到别人的会话上。
        //
        //   所以必须同时校验「还在表里」且「还是同一个对象」。
        {
            auto it = _conns.find(fd);
            if (it == _conns.end() || it->second != conn) return;
        }

        // ① wbuf 里还有残留 → 新数据必须排到队尾，保证字节顺序
        if (!conn->wbuf_empty()){
            conn->append_wbuf(static_cast<const char*>(data), n);
            return;
        }

        // ② 直发
        size_t  remaining = n;
        while (remaining > 0){
            const ssize_t w = ::send(fd,
                                     static_cast<const char*>(data) + (n - remaining),
                                     remaining, MSG_NOSIGNAL);
            if (w > 0){
                remaining -= static_cast<size_t>(w);
            } else if (w == 0){
                // TCP 上 send 返回 0 无法推进，继续循环就是死转
                close_conn(fd);
                return;
            } else {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;   // 转 wbuf
                close_conn(fd);                                      // EPIPE / ECONNRESET
                return;
            }
        }

        // ③ 没发完 → 残余入 wbuf + 打开 EPOLLOUT
        if (remaining > 0){
            conn->append_wbuf(static_cast<const char*>(data) + (n - remaining), remaining);
            _epoll->mod(fd, EPOLLIN | EPOLLOUT);
        }
    }

    void handle_write(int fd){
        auto it = _conns.find(fd);
        if (it == _conns.end()) return;
        std::shared_ptr<Connection> conn = it->second;

        while (!conn->wbuf_empty()){
            const ssize_t w = ::send(fd, conn->wbuf_data(), conn->wbuf_size(), MSG_NOSIGNAL);
            if (w > 0){
                conn->wbuf_retrieve(static_cast<size_t>(w));
                continue;
            }
            if (w < 0 && errno == EINTR) continue;
            if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
                _epoll->mod(fd, EPOLLIN | EPOLLOUT);   // 还是满，继续等
                return;
            }
            close_conn(fd);
            return;
        }

        // wbuf 排空 → 摘掉 EPOLLOUT（不做就会 busy loop：一直可写、一直空转）
        _epoll->mod(fd, EPOLLIN);
    }

    // ------------------------------------------------------------------
    //  拆连接
    // ------------------------------------------------------------------
    void close_conn(int fd){
        // ★ 顺序：先 del 再 close(fd)。
        //   反过来会有一个窗口：close 后 fd 号被 accept 重新用掉，
        //   此时 epoll_ctl(DEL, 同一个号) 摘掉的是【新连接】的 epitem ——
        //   新连接从此从 epoll 消失，不报错、不关闭、客户端超时。
        _epoll->del(fd);
        if (_on_close) _on_close(_id, fd);
        auto it = _conns.find(fd);
        if (it != _conns.end()) _conns.erase(it);   // 引用归零 → ~Connection → ::close
    }

    // ------------------------------------------------------------------
    //  跨线程队列
    // ------------------------------------------------------------------
    void wakeup(){
        if (_wake_fd < 0) return;
        const uint64_t one = 1;
        const ssize_t  ignore = ::write(_wake_fd, &one, sizeof(one));
        (void)ignore;
    }

    void handle_wakeup(){
        uint64_t cnt = 0;
        // eventfd 是 8 字节计数器，要读到 EAGAIN 才算清零
        while (::read(_wake_fd, &cnt, sizeof(cnt)) == static_cast<ssize_t>(sizeof(cnt))) {}
        do_pending();
    }

    void do_pending(){
        std::vector<Task> batch;
        {
            std::lock_guard<std::mutex> lk(_q_mtx);
            if (_q.empty()) return;
            batch.swap(_q);     // swap：锁只拿一瞬，不阻塞生产者
        }
        for (auto& t : batch){
            try{ t(); }
            catch (const std::exception& e){ std::cerr << "task threw: " << e.what() << '\n'; }
            catch (...){ std::cerr << "task threw unknown\n"; }
        }
    }

    // ------------------------------------------------------------------
    size_t _id;
    TcpServerBase::FrameCallback        _frame_cb;
    std::function<void(size_t,int)>     _on_open;    // 通知主 Reactor：fd 归属变了
    std::function<void(size_t,int)>     _on_close;

    std::unique_ptr<Epoll> _epoll;
    int                    _wake_fd = -1;

    // ★ 线程私有，全部无锁
    std::unordered_map<int, std::shared_ptr<Connection>> _conns;

    // 业务线程池（可选）。nullptr = 不挂池子，业务直接在 I/O 线程里跑。
    // 声明在 _conns 之后 ⇒ 析构时先于 _conns 销毁；析构函数里还会显式 shutdown 一遍兜底。
    std::unique_ptr<IoThreadPool> _pool;

    // 生产者可能来自任意线程，只有这一把锁
    std::mutex           _q_mtx;
    std::vector<Task>    _q;

    std::thread          _th;
    std::atomic<bool>    _running{true};

    // 每个线程自己知道自己是谁的 Reactor
    static thread_local SubReactor* tls_current;
};

inline thread_local SubReactor* SubReactor::tls_current = nullptr;


// ============================================================================
//  OLPT_Server —— 主 Reactor：只 accept，不碰任何业务
// ============================================================================
class OLPT_Server : public TcpServerBase{
public:
    static constexpr int    kMaxEvents = 1024;
    static constexpr int    kWaitMs    = 100;

    /**
     * @param port         监听端口
     * @param n_sub        从 Reactor 数量（真正决定 I/O 并行度的是这个）
     * @param pool_threads 每个从 Reactor 挂几个业务线程
     *                     1 = 标准形态（推荐）；0 = 不挂（业务在 I/O 线程里跑，对照组）
     * @param queue_cap    每个业务池的待办队列容量
     */
    explicit OLPT_Server(int port,
                         size_t n_sub = std::thread::hardware_concurrency(),
                         size_t pool_threads = 1,
                         size_t queue_cap = 1024)
        : TcpServerBase(port)
        , _n_sub(n_sub == 0 ? 1 : n_sub)
        , _pool_threads(pool_threads)
        , _queue_cap(queue_cap == 0 ? 1 : queue_cap)
    {}

    ~OLPT_Server() override {
        // 先停从 Reactor：它们的线程还在跑 _frame_cb，
        // 不能让它们在本对象成员析构之后才结束
        for (auto& s : _subs) if (s) s->stop();
        _subs.clear();
        { std::lock_guard<std::mutex> lk(_own_mtx); _owner.clear(); }
    }

    size_t sub_count() const noexcept { return _n_sub; }

protected:
    // ------------------------------------------------------------------
    //  主 Reactor 主循环
    // ------------------------------------------------------------------
    void run_loop() override {
        if (!init_listen_nonblock()) return;

        _epoll = std::make_unique<Epoll>(EPOLL_CLOEXEC, kMaxEvents);
        if (!_epoll->add(_listen_fd, EPOLLIN)){
            std::cerr << "epoll add listen failed\n";
            return;
        }

        start_subs();

        std::vector<struct epoll_event> evs(kMaxEvents);
        while (_running){
            const int n = _epoll->wait(evs, kWaitMs);
            if (n < 0) break;

            for (int i = 0; i < n; ++i){
                const int      fd = evs[i].data.fd;
                const uint32_t ev = evs[i].events;
                if (fd != _listen_fd) continue;
                if (ev & (EPOLLERR | EPOLLHUP)){
                    std::cerr << "listen fd error\n";
                    _running = false;
                    break;
                }
                handle_accept();
            }
        }

        for (auto& s : _subs) if (s) s->stop();
    }

    // ------------------------------------------------------------------
    //  send 门面：把字节路由到 fd 的属主线程
    // ------------------------------------------------------------------
    void send_wbuf(int fd, const void* data, size_t n) override {
        if (fd < 0 || data == nullptr || n == 0) return;

        SubReactor* sub = nullptr;
        {
            std::lock_guard<std::mutex> lk(_own_mtx);
            auto it = _owner.find(fd);
            if (it == _owner.end()) return;          // 连接已死，丢弃
            sub = it->second;
        }

        const std::string payload(static_cast<const char*>(data), n);
        sub->run_in_loop([sub, fd, payload]{
            sub->send(fd, payload.data(), payload.size());
        });
    }

private:
    bool init_listen_nonblock(){
        int flags = ::fcntl(_listen_fd, F_GETFL);
        if (flags == -1){
            std::cerr << "fcntl F_GETFL: " << std::strerror(errno) << '\n';
            return false;
        }
        if (::fcntl(_listen_fd, F_SETFL, flags | O_NONBLOCK) == -1){
            std::cerr << "fcntl F_SETFL: " << std::strerror(errno) << '\n';
            return false;
        }
        return true;
    }

    void start_subs(){
        _subs.reserve(_n_sub);
        for (size_t i = 0; i < _n_sub; ++i){
            // on_open / on_close 在【从线程】里执行。
            // 回调第一个参数就是 Reactor 的 id，所以直接 _subs[id] 就能拿到属主，
            // 不需要在 accept 那边暂存「这个 fd 要交给谁」——
            // 谁执行的这段代码，谁就是属主，信息本来就带在调用栈里。
            _subs.emplace_back(std::make_unique<SubReactor>(
                i, _frame_cb,
                [this](size_t id, int fd){
                    std::lock_guard<std::mutex> lk(_own_mtx);
                    _owner[fd] = _subs[id].get();
                },
                [this](size_t /*id*/, int fd){
                    std::lock_guard<std::mutex> lk(_own_mtx);
                    _owner.erase(fd);
                },
                _pool_threads, _queue_cap));
        }
        for (auto& s : _subs) s->start();
    }

    void handle_accept(){
        for (;;){
            const int cfd = ::accept4(_listen_fd, nullptr, nullptr,
                                      SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (cfd < 0){
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;   // 队列空了
                if (errno == EINTR) continue;
                if (errno == EMFILE || errno == ENFILE){
                    std::cerr << "fd exhausted: " << std::strerror(errno) << '\n';
                    break;
                }
                std::cerr << "accept4: " << std::strerror(errno) << '\n';
                break;
            }
            dispatch(cfd);
        }
    }

    /** 轮询分发：只入队，绝不在这里碰从 Reactor 的表 */
    void dispatch(int cfd){
        if (_subs.empty()){ ::close(cfd); return; }

        SubReactor* sub = _subs[_next].get();
        _next = (_next + 1) % _subs.size();          // 主线程独占，无需原子

        // run_in_loop 是唯一合法的跨线程入口：
        // adopt() 里会改 _conns / epoll，那些只允许在属主线程里动。
        sub->run_in_loop([sub, cfd]{ sub->adopt(cfd); });
    }

    size_t _n_sub;
    size_t _next = 0;

    // 每个从 Reactor 的业务池参数（只在 start_subs 时读一次）
    size_t _pool_threads;
    size_t _queue_cap;

    std::unique_ptr<Epoll>                   _epoll;
    std::vector<std::unique_ptr<SubReactor>> _subs;

    // fd → 属主从 Reactor，供 send 路由（跨线程读）
    std::mutex                           _own_mtx;
    std::unordered_map<int, SubReactor*> _owner;
};
