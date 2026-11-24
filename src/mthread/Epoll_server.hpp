#pragma once
#include "server/tcp_server.hpp"
#include "Iothread_pool.hpp"
#include <unordered_map>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <cstring>
#include "Connection.hpp"
#include "epoll.hpp"
#include "timer/idle_scanner.hpp"   // 空闲超时：TimerQueue 的接线层
#include <chrono>
#include <cerrno>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

/**
 * g++ -std=c++17 -fsyntax-only -I./lib -I./src src/mthread/Epoll_server.hpp
 * 
 */

class EpollServer:public TcpServerBase{
    /** private: 只能由该类中的函数、其友元函数访问,不能被任何其他访问，该类的对象也不能访问.
        protected: 可以被该类中的函数、子类的函数、以及其友元函数访问,但不能被该类的对象访问
        public: 可以被该类中的函数、子类的函数、其友元函数访问,也可以由该类的对象访问
     */

private:
    /**
     * ★ worker -> reactor 的回写任务
     *
     * 为什么 worker 不自己 send：
     *   wbuf（发送缓冲区）和 epoll_ctl 必须由 reactor 单线程独占，理由见 send_wbuf。
     *   所以 worker 只把「要发的字节」打包塞进这个结构，剩下的交给 reactor 做。
     */
    struct PendingWrite{
        std::shared_ptr<Connection> conn;
        std::string data;
    };

public:
    static constexpr int kMaxEvents = 1024;
    static constexpr int kWaitTimeOut = 100;
    static constexpr size_t KReadBuf = 4096;
    // 一次 EPOLLIN 最多读这么多字节，读完就返回让别的连接先跑。
    // 没有这个配额，一个灌数据的快客户端能让 reactor 卡在 while 里出不来。
    static constexpr size_t kReadQuota = 64 * 1024;

    /**
     * @param port      监听端口
     * @param workers   业务线程数（线程池的 worker 数）
     * @param queue_cap 每个 worker 待办队列的容量（满了 = 背压，会断连接）
     */
    explicit EpollServer(int port,size_t workers = 4,size_t queue_cap = 1024)
        :TcpServerBase(port)
        ,_workers(workers == 0 ? 1 : workers)
        ,_queue_cap(queue_cap == 0 ? 1 : queue_cap)
        ,_reactor_tid(std::this_thread::get_id())
    {
        /**
         * ★ 分片线程池：N 个「单线程池」，连接按 fd % N 固定归属。
         *
         * 为什么不直接用 1 个 IoThreadPool(N)：
         *   同一个连接的两帧可能被两个 worker 同时处理，
         *   回包顺序就乱了 —— throughput（流水线）模式下客户端逐条比对会 mismatch。
         *   按 fd 取模分片后，一条连接的所有帧永远落进同一个线程，
         *   队列是 FIFO + 单消费者 => 回包顺序 = 请求顺序。
         *
         * 这就是 nginx 多 worker 的雏形（阶段 4 的 SO_REUSEPORT 分片是同一思想）。
         */
        for(size_t i = 0;i < _workers;++i){
            _pools.emplace_back(std::make_unique<IoThreadPool>(1,_queue_cap));
        }
    }

    ~EpollServer() override{
        // ★ 必须先关池子：worker 可能还在跑 _frame_cb / send_wbuf，
        //   让它们在本对象的成员析构之后才结束 = 访问已释放内存。
        shutdown_pools();
        if(_wake_fd >= 0){::close(_wake_fd);_wake_fd = -1;}
    }

    /**
     * @brief 覆盖基类：除了记下阈值，还要同步给扫描器
     *
     * ★ 必须在 start() 之前调用（bench_server 就是这个顺序）。
     *   如果 run_loop 已经跑起来再改，已经在定时器里的旧 deadline 不会跟着变，
     *   所以这里同时转发给 _scanner。
     */
    void set_idle_timeout(std::chrono::milliseconds ms) override{
        TcpServerBase::set_idle_timeout(ms);
        if (_scanner) _scanner->set_timeout(ms);
    }

    /**
     * @brief 空闲超时是否真的生效
     *
     * ★ 用来消灭缺陷 C（假通过）：bench_server 以前只探测「有没有
     *   set_idle_timeout 这个方法」，而基类早就有了 → 探测永远为真。
     *   现在改成探测这个，没接线就是 false，06_timer.sh 才能真的红。
     */
    bool supports_idle_timeout() const noexcept{ return _scanner != nullptr; }

    void send_wbuf (int __fd,const void* __data,size_t __n)override{
        if(__fd < 0 || __data == nullptr || __n == 0) return;

        //缓冲区有空位 → 塞进去 → 成功；缓冲区满了 → EAGAIN → 剩下的塞不进 → 转到 wbuf 排队等。

        std::shared_ptr<Connection> conn_ptr;
        {
            std::lock_guard<std::mutex> lk(_mtx);

            auto it = _conns.find(__fd);
            if (it != _conns.end())
            {
                conn_ptr = it->second;
            }
        }

        // ★ 连接可能已经被 handle_read 里的 del_and_close 摘掉了
        //   （比如对端先 close，业务回调里还在发），此时直接丢弃。
        if (!conn_ptr) return;

        // ① 调用者就是 reactor 自己 → 直接走 flush，零开销
        if (std::this_thread::get_id() == _reactor_tid){
            flush(conn_ptr,__data,__n);
            return;
        }

        // ② 调用者是 worker 线程 → 绝对不能碰 wbuf 和 epoll_ctl
        //
        //    epoll_ctl(MOD) 本身是线程安全的，但语义不安全：
        //    reactor 刚把这个 fd mod 成 EPOLLIN|EPOLLOUT，
        //    worker 又 mod 回 EPOLLIN —— 谁后写谁生效。
        //    一旦 worker 后写，EPOLLOUT 就丢了，wbuf 永远等不到冲刷的时机。
        //    同理，两个 worker 同时 append_wbuf 会把两条消息的字节搅在一起。
        //
        //    所以跨线程只做一件事：字节入队 + eventfd 叫醒 reactor，
        //    真正的 send / wbuf / epoll_ctl 全部由 reactor 单线程完成。
        {
            std::lock_guard<std::mutex> lk(_wq_mtx);
            _wq.push_back(PendingWrite{conn_ptr,
                std::string(static_cast<const char*>(__data),__n)});
        }
        wakeup();
    }
    
protected:

    void run_loop() override{
        // ★ 在这里而不是构造函数里记 reactor 线程：
        //   构造和 start() 完全可能发生在不同线程上（比如构造完交给别的线程跑）。
        //   记错了 -> 真正的 reactor 走「跨线程入队」分支 -> wbuf 永远没人冲刷 -> 挂死。
        _reactor_tid = std::this_thread::get_id();

        if(!epoll_init()){
            std::cerr<<"epoll_init failed"<<std::endl;
            return;
        }
        _epoll.get()->set_max_events(kMaxEvents);

        // ★ 定时器在这里才真正创建：
        //   --idle-timeout 在 start() 之前就设好了，构造期 _idle_ms 已是对的。
        //   构造期建 scanner 也能工作，但 run_loop 里建保证「定时器活在 reactor 线程」。
        _scanner = std::make_unique<IdleScanner>(
            std::make_unique<ScanTimer>(), _idle_ms);

        // 下一轮该扫的时间点。用它而不是「每轮都扫」：
        // epoll_wait 可能因为收到数据而频繁返回，每轮扫 = 高频空转。
        TimerPoint next_scan = TimerClock::now() + kTimerTick;

        while(_running){
            //查看有多少活跃事件并返回
            ssize_t n = _epoll.get()->wait(event_poll,kWaitTimeOut);
            if (n < 0) {
                // ★ 缺陷 K：EINTR 不是错误（信号打断而已）。
                //   原来直接 break -> 一个无关信号就能让整个 reactor 退出，
                //   而 _running 还是 true，外面看起来「服务在跑但没反应」。
                if (errno == EINTR) continue;
                break;
            }
            for(ssize_t i = 0; i < n;++i){
                int fd = event_poll[i].data.fd;
                const uint32_t ev = event_poll[i].events;

                // ★ worker 回写唤醒：把队列里的结果统一 flush。
                //   fd 一定要第一个判断 —— eventfd 不在 _conns 里，
                //   掉进下面的分支只会打印一堆无意义的 epoll_ctl 错误。
                if (fd == _wake_fd) {
                    handle_wakeup();
                    continue;
                }

                if (fd == _listen_fd) {
                    handle_accept();
                    continue;
                }

                // ★ EPOLLERR/EPOLLHUP 优先：连接已死就没必要读写，
                //   且它常与 EPOLLIN 同时置位，放在 else-if 链末尾会被吞掉
                if (ev & EPOLLERR) {
                    del_and_close(fd);
                    continue;
                }

                // EPOLLHUP：对端 close。但内核里可能还残留未读完的数据，
                // 先读一次把数据榨干，读到 0 时 handle_read 内部会关闭。
                if (ev & EPOLLHUP) {
                    if (ev & EPOLLIN) handle_read(fd);
                    else              del_and_close(fd);
                    continue;
                }

                // ★ EPOLLIN / EPOLLOUT 用两个独立 if，不互斥：
                //   同一个 fd 完全可能同时可读可写
                if (ev & EPOLLIN)  handle_read(fd);
                if (ev & EPOLLOUT) handle_write(fd);
            }

            // ---- 定时扫描：到点才扫，不是每轮都扫 ----
            const TimerPoint now = TimerClock::now();
            if (now >= next_scan) {
                evict_idle(now);
                next_scan = now + kTimerTick;
            }
            //while (_pool.try_run_one())
        }
    }

private:

    //flush :新数据、纯冲刷、EAGAIN 落 wbuf、发完摘 EPOLLOUT。
    // ★ 只允许 reactor 线程调用（wbuf / epoll_ctl 的独占者）。
    void flush(std::shared_ptr<Connection> conn_ptr,const void* __data,size_t __n){

        // worker 算完要回包时，连接可能已经被 reactor 关掉了，白跑一趟不如直接退
        if (!conn_ptr || conn_ptr->closed()) return;

        /**
         * ★ fd 复用防护（跨线程最容易踩的坑，比 UAF 更隐蔽）
         *
         * 只判 conn->closed() 是不够的：worker 手里的 shared_ptr 会延长
         * Connection 的寿命，所以 reactor 那边 del_and_close 摘掉连接时，
         * 这个对象的 fd 其实还开着 —— closed() 返回 false。
         *
         * 而一旦 fd 已经不在 _conns 里，就说明 reactor 认定这条连接死了。
         * 此时若还往里 send，最好的情况是 EPIPE，最坏的情况是
         * 内核已经把 fd 号发给了一条新连接 —— 数据直接串到别人会话上。
         *
         * 所以必须同时校验「还在表里」且「还是同一个对象」。
         */
        {
            std::lock_guard<std::mutex> lk(_mtx);
            auto it = _conns.find(conn_ptr->fd());
            if (it == _conns.end() || it->second != conn_ptr) return;
        }

        //1.如果wbuf里有上一次没发送完的数据，这次的数据排队到末尾
        /**
         * 状态：wbuf 空 + 未注册 EPOLLOUT      ← 空闲态
                │ send() 全部塞进内核成功
                ▼
        内核缓冲区满 → 剩余存 wbuf + 注册 EPOLLOUT
                ▼
        对端 read → TCP 窗口滑动 → 内核缓冲区腾出空间
                ▼
        EPOLLOUT 触发 → flush：把 wbuf 全部 write() 出去
                ▼
        wbuf 空了 → 注销 EPOLLOUT           ← 回到空闲态
         * 
         * 
         */
        if(!conn_ptr->wbuf_empty()){
            conn_ptr->append_wbuf(static_cast<const char*>(__data),__n);
            return;
        }

        //2.如果wbuf为空，尝试直接调用send发送
        size_t remaining = __n;
        ssize_t nwritten = 0;
        bool error = false;

        while(remaining > 0){
            nwritten = ::send(conn_ptr->fd(), static_cast<const char*>(__data) + (__n - remaining), remaining, MSG_NOSIGNAL);
            if (nwritten > 0) {
                remaining -= static_cast<size_t>(nwritten);
                // ★ 刷新点 ②：send 真的发出去字节了
                //   这一处漏掉，「只推不读」的订阅类连接会被活活误杀 ——
                //   它一直在收通知，但从不发数据，只在 recv 刷就永远不会被刷。
                conn_ptr->touch();
                if (_scanner) _scanner->touch(conn_ptr->fd());
            } else if (nwritten == 0) {
                // ★ send 返回 0 在 TCP 上意味着无法推进，继续循环就是死转
                //   （remaining 不变）。当成错误处理。
                error = true;
                break;
            } else {
                if (errno == EINTR) continue; // 信号打断，重试
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break; // 内核缓冲区满了，退出循环，转 wbuf
                }
                error = true; // 真正出错（如 EPIPE / ECONNRESET）
                break;
            }
        }

        if(error){
            del_and_close(conn_ptr->fd());
            return;
        }

        // 如果没发完就把剩下的残余数据塞进wbuf，并让epoll监听可写事件
        if(remaining > 0){
            conn_ptr->append_wbuf(static_cast<const char*>(__data) + (__n - remaining),remaining);
            _epoll->mod(conn_ptr->fd(),EPOLLIN|EPOLLOUT);
        }
    }

    bool epoll_init(){
        event_poll.reserve(kMaxEvents);

        //设置listen_fd为非阻塞模式
        int flags = fcntl(_listen_fd,F_GETFL);
        if(flags == -1){
            std::cerr<<"fcntl F_GETFL failed: "<<strerror(errno)<<std::endl;
            return false;
        }

        if(fcntl(_listen_fd,F_SETFL,flags|O_NONBLOCK) == -1){
            std::cerr<<"fcntl F_SETFL failed: "<<strerror(errno)<<std::endl;
            return false;
        }

        //初始化一个epoll实例
        _epoll = std::make_unique<Epoll>(EPOLL_CLOEXEC,kMaxEvents);

        // ★ eventfd：worker 算完后用它把 reactor 从 epoll_wait 里立刻叫醒。
        //   没有它，跨线程回包只能等 kWaitTimeOut(100ms) 超时，
        //   延迟凭空多出一到两个数量级 —— 压测里会表现成 p50 抖到几十毫秒。
        _wake_fd = ::eventfd(0,EFD_NONBLOCK | EFD_CLOEXEC);
        if (_wake_fd < 0){
            std::cerr<<"eventfd: "<<std::strerror(errno)<<std::endl;
            return false;
        }
        if (!_epoll.get()->add(_wake_fd,EPOLLIN)) return false;

        return  _epoll.get()->add(_listen_fd);
    }
    
    void handle_accept(){
        for(;;){
            //addr和addrlen都设置为nullptr是在只需要建立一个新链接拿到文件描述符不检验ip地址时的合法操作
            int cfd = ::accept4(_listen_fd,nullptr,nullptr,SOCK_NONBLOCK | SOCK_CLOEXEC);

            if(cfd < 0){
                if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }  // 队列空了，正常
                if (errno == EINTR) { continue; }                        // 信号打断
                if (errno == EMFILE || errno == ENFILE) {
                    std::cerr << "fd exhausted: " << std::strerror(errno) << '\n';
                    break;
                }
                std::cerr << "accept4: " << std::strerror(errno) << '\n';
                break;
            }
            {
                std::lock_guard<std::mutex> lk(_mtx);
                _conns.emplace(cfd,std::make_shared<Connection>(cfd));
            }
            if(!_epoll.get()->add(cfd)){
                del_and_close(cfd);
            }else if (_scanner){
                // ★ 新连接一建就进定时器，不能等第一次 recv 才注册：
                //   「连上但一直不说话」的连接永远不会有 recv，
                //   不在这里注册就永远不会被扫到 → 06_timer.sh 阶段 [1] 必红。
                _scanner->touch(cfd);
            }
        }
    }
    void handle_read(int fd){
        
        char buf[KReadBuf];
        // ★ 读配额：一次事件最多搬 kReadQuota 字节就还给别人。
        //   LT 模式下没读完的数据下次 EPOLLIN 还会再报，属于安全的"让路"。
        size_t budget = kReadQuota;

        while(budget > 0){

            ssize_t n = ::recv(fd,buf,sizeof(buf),0);
            if (n > 0) {
                budget = (static_cast<size_t>(n) >= budget) ? 0
                                                           : budget - static_cast<size_t>(n);
                /**
                 * 这里注意一下，使用RPC后不只有原来的内核缓冲区了，现在是内核缓冲区➕TLVDecoder内的缓冲区
                 * 内核缓冲区只能按照TCP协议保证字节顺序不乱，字节不丢，至于这是不是一帧它并不知道，所以新创立一个decoder来存半帧的情况
                 */

                std::shared_ptr<Connection> conn;
                {
                    std::lock_guard<std::mutex> lk(_mtx);
                    auto it = _conns.find(fd);
                    if(it == _conns.end())return;
                    conn = it->second;
                }
                // ★ 刷新点 ①：recv 真的读到字节了
                conn->touch();
                if (_scanner) _scanner->touch(fd);
                conn->decoder().appendData(buf,static_cast<size_t>(n));

                FrameHeader hdr;
                std::string body;
                
                for(;;){
                    auto st = conn->decoder().tryDecode(hdr,body);
                    
                    //出口1， 流不可信 关连接
                    if(st == TLVDecoder::Status::BAD_ERROR){
                        del_and_close(fd);
                        return;     // ★ 必须直接 return：连接已经关了，再 tryDecode 就是空转
                    }

                    //校验和错：分帧本身没问题，只丢这一帧，流可以继续用
                    if(st == TLVDecoder::Status::ERROR)continue;
                    //出口2，没完整帧了 正常结束，等下一次EPOLLIN （主要都是在needmore中退出）
                    if(st == TLVDecoder::Status::NEED_MORE )break;
                    
                    //出口3，完整一帧 —— 但业务不在这里跑，扔给线程池
                    if(st == TLVDecoder::Status::OK){
                        if(!dispatch_frame(conn,hdr,body)){
                            return;     // 池子满了，dispatch_frame 内部已经断开连接
                        }
                    }
                }
            }else if(n == 0){
                del_and_close(fd);
                return;
            }else {
                if (errno == EINTR) continue; // 被信号打断，重试
                if (errno == EAGAIN || errno == EWOULDBLOCK){
                    return;
                }
                del_and_close(fd);
                return;
            }
        }
        // 走到这里 = 配额用完。剩下的数据留在内核里，下次 EPOLLIN 继续搬。
    }

    // ------------------------------------------------------------------
    //  reactor <-> worker 的桥
    // ------------------------------------------------------------------

    /**
     * ★ 把「一帧完整消息」交给线程池
     *
     * 捕获的是 shared_ptr<Connection> 而不是裸 fd：
     *   1) 持有 shared_ptr 期间连接对象不会被析构 => fd 不会被 close =>
     *      内核不可能把这个 fd 号发给新连接 => 不存在「写到别人连接上」的经典 fd 复用 bug；
     *   2) fd 在任务开始时就固定下来，之后即便 reactor 把连接摘了也不会错位。
     *
     * @return false 表示待办队列满（背压），此时连接已被断开
     */
    bool dispatch_frame(const std::shared_ptr<Connection>& conn,
                        const FrameHeader& hdr,const std::string& body){
        const int fd = conn->fd();
        if(fd < 0 || _pools.empty()) return false;

        // 按 fd 取模分片：同一条连接永远进同一个 worker => 回包天然有序
        const size_t shard = static_cast<size_t>(fd) % _pools.size();

        auto task = [this,fd,conn,hdr,body](){
            if(!_frame_cb) return;
            if(conn->closed()) return;      // 期间被 reactor 关了，别浪费 CPU
            _frame_cb(fd,hdr,body);
        };

        if(_pools[shard]->submit(std::move(task))) return true;

        // ★ 背压：这一帧已经从 decoder 里消费掉了，丢掉它 = 流损坏，
        //   客户端会收到半截/错位的包。宁可断开连接，也不能发出错误数据。
        //   （阶段 3 要做的事：用读配额/暂停 EPOLLIN 代替直接断连）
        del_and_close(fd);
        return false;
    }

    // worker 通过 eventfd 叫醒 reactor 后，在这里把结果统一写回
    void handle_wakeup(){
        uint64_t cnt = 0;
        // eventfd 是计数器，要一直读到 EAGAIN 才算清零
        while(::read(_wake_fd,&cnt,sizeof(cnt)) == static_cast<ssize_t>(sizeof(cnt))){}

        std::vector<PendingWrite> batch;
        {
            std::lock_guard<std::mutex> lk(_wq_mtx);
            batch.swap(_wq);        // swap 而不是拷贝：锁只拿一小会儿，不阻塞 worker 继续 enqueue
        }
        for(auto& item : batch){
            flush(item.conn,item.data.data(),item.data.size());
        }
    }

    void wakeup(){
        uint64_t one = 1;
        ssize_t ignore = ::write(_wake_fd,&one,sizeof(one));
        (void)ignore;   // 非阻塞 eventfd 理论上不会失败；就算失败也只是退化成等超时
    }

    void shutdown_pools(){
        for(auto& p : _pools){
            if(p) p->shutdown();        // join 所有 worker，返回后保证没有任务在跑
        }
        _pools.clear();
    }
    void handle_write(int fd){
        std::shared_ptr<Connection> conn_ptr;
        {
            std::lock_guard<std::mutex> lk(_mtx);
            auto it = _conns.find(fd);
            if(it==_conns.end()) return;
            conn_ptr = it->second;
        }
        while (!conn_ptr->wbuf_empty())
        {
            ssize_t n = ::send(fd, static_cast<const char*>(conn_ptr->wbuf_data()),conn_ptr->wbuf_size(), MSG_NOSIGNAL);
            if(n>0){
                conn_ptr->wbuf_retrieve(static_cast<size_t>(n));
                // ★ 刷新点 ③：EPOLLOUT 驱动的冲刷也算活跃
                //   这里的 n>0 是「积压数据终于发出去了」，
                //   典型场景：客户端读得慢，一段时间后恢复。
                conn_ptr->touch();
                if (_scanner) _scanner->touch(fd);
                continue;
            }else{
                if(errno == EAGAIN){
                    //发送缓冲区满了剩余数据存 wbuf，注册 EPOLLOUT 等通知
                    //EPOLLOUT 驱赶 wbuf，EPOLLIN 保证还能收到新数据
                    _epoll->mod(fd,EPOLLOUT|EPOLLIN);
                    return;
                }else{
                    del_and_close(fd);
                    return;
                }
            }
            
        }
        if(conn_ptr -> wbuf_empty()){
            _epoll->mod(fd,EPOLLIN);//这里是摘掉EPOLLOUT的操作，直接置为EPOLLIN
            return;
        }
        
    } 

    void del_and_close(int fd){
        // ★ 先摘定时器记录，再关 fd。顺序很重要：
        //   fd 关掉后号会被内核复用，若定时器里还留着旧记录，
        //   下一次扫描会把这个号「踢」一次 —— 而它现在是别人的连接。
        //   （evict_idle 路径下 collect 已经摘过，这里返回 false，无害）
        if (_scanner) _scanner->remove(fd);

        _epoll.get()->del(fd);
        std::lock_guard<std::mutex> lk (_mtx);
        _conns.erase(fd);
    }

    /**
     * ★ 空闲连接回收 —— 全项目最容易写错的一段，只有 6 行核心逻辑
     *
     * 正确顺序（记住这条主线，其他都是注释）：
     *     持锁只读收集  →  放锁  →  逐个 del_and_close
     *
     * 为什么不能持锁调 del_and_close（两个具体后果）：
     *   ① 死锁：del_and_close 内部要 lock_guard(_mtx)，
     *      而 std::mutex 不可重入 —— 同一个线程第二次 lock 就是永久自锁，
     *      整个 reactor 当场停摆（进程在、CPU 0%、端口还在 listen）。
     *   ② 迭代器失效 / 崩溃：就算换成 recursive_mutex 让它不死锁，
     *      你在遍历 _conns 的过程中 erase 当前元素，
     *      被删元素的迭代器立即失效，下一次 ++it 就是 UB。
     *
     * 为什么定时器里已经摘掉了还要再 del_and_close：
     *   collect() 只负责「定时器不再盯着这个 fd」，fd 还开着、还在 epoll 里、
     *   还在 _conns 里。不在这里关掉就是 fd 泄漏。
     *
     * 放锁后 fd 可能已被别人删掉（worker 的 flush 发现 EPIPE 也会调
     * del_and_close）。del_and_close 本身是幂等的（epoll_ctl DEL 失败
     * 只是返回错误、_conns.erase 不存在的 key 也安全），所以重复删除无害。
     */
    void evict_idle(TimerPoint now){
        if (!_scanner || !_scanner->enabled()) return;

        // ---- ① 持锁只读收集 ----
        // 局部 vector，生命周期只到函数结束；锁的范围严格止于收集完。
        IdleScanner::IdList expired;
        {
            std::lock_guard<std::mutex> lk(_mtx);
            _scanner->collect(now, expired);
        }
        if (expired.empty()) return;

        // ---- ② 放锁，逐个删 ----
        // 这里 _mtx 已经释放，del_and_close 里再 lock 完全没问题。
        for (int fd : expired) {
            del_and_close(fd);
        }
    }

    // ★ 成员声明顺序 = 构造顺序，析构顺序相反。
    //   _pools 声明在 _conns/_mtx 之后 => 析构时先于它们销毁，
    //   worker 就不可能访问已经析构的 _conns。（析构函数里还会先 shutdown 一次，双保险）
    std::unique_ptr<Epoll> _epoll;
    std::vector<struct epoll_event> event_poll;

    //map用于通过文件描述符（fd）快速查找对应 epoll_item（监听事件）的关联结构。
    std::unordered_map<int,std::shared_ptr<Connection>> _conns;
    std::mutex _mtx;

    // 空闲超时扫描器。只在 reactor 线程里读写（start 之前配置除外），
    // 和 wbuf / epoll_ctl 一样属于「reactor 独占资源」。
    std::unique_ptr<IdleScanner> _scanner;

    // 分片业务线程池：_pools[i] 只有一个 worker，负责 fd % N == i 的连接
    std::vector<std::unique_ptr<IoThreadPool>> _pools;
    size_t _workers;
    size_t _queue_cap;

    // worker -> reactor 回写通道
    int _wake_fd = -1;                       // eventfd
    std::mutex _wq_mtx;
    std::vector<PendingWrite> _wq;
    std::thread::id _reactor_tid;            // 谁是 reactor 线程（run_loop 的执行者）
};


