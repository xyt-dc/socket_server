//--------------------------------uniqur_ptr-----------------------------------
//unique_ptr 独占所有权，RAII（资源定义即初始化），禁止拷贝构造、拷贝赋值，支持移动构造，支持移动赋值
#include <utility>

template <typename T>
class SimpleUniquePtr {
public:
    explicit SimpleUniquePtr(T* p = nullptr) noexcept : ptr_(p) {}

    ~SimpleUniquePtr() { delete ptr_; }

    // 禁用拷贝构造与拷贝赋值
    SimpleUniquePtr(const SimpleUniquePtr&) = delete;
    SimpleUniquePtr& operator=(const SimpleUniquePtr&) = delete;

    // 支持移动构造
    SimpleUniquePtr(SimpleUniquePtr&& other) noexcept
        : ptr_(other.ptr_) {
        other.ptr_ = nullptr;
    }

    // 支持移动赋值
    SimpleUniquePtr& operator=(SimpleUniquePtr&& other) noexcept {
        if (this != &other) {
            delete ptr_;
            ptr_ = other.ptr_;
            other.ptr_ = nullptr;
        }
        return *this;
    }

    T& operator*() const noexcept { return *ptr_; }
    T* operator->() const noexcept { return ptr_; }
    T* get() const noexcept { return ptr_; }

    T* release() noexcept {
        T* tmp = ptr_;
        ptr_ = nullptr;
        return tmp;
    }

    void reset(T* p = nullptr) noexcept {
        if (ptr_ != p) {
            delete ptr_;
            ptr_ = p;
        }
    }

private:
    T* ptr_ = nullptr;
};

//----------------------------------shared_ptr---------------------------------------
// shared_ptr 的最简实现核心是共享所有权与引用计数：多个指针共享同一块堆内存，
//并在堆上额外维护一个计数器，只有当最后一个指针销毁（计数归零）时才释放内存。
#include <cstddef>

template <typename T>
class SimpleSharedPtr {
public:
    // 1. 构造：接管新指针，同时在堆上创建引用计数（初始为 1）
    explicit SimpleSharedPtr(T* p = nullptr)
        : ptr_(p), count_(p ? new size_t(1) : nullptr) {}

    // 2. 析构：释放当前一份所有权
    ~SimpleSharedPtr() {
        release_ref();
    }

    // 3. 拷贝构造：共享指针与计数器，计数 +1
    SimpleSharedPtr(const SimpleSharedPtr& other) noexcept
        : ptr_(other.ptr_), count_(other.count_) {
        if (count_) {
            ++(*count_);
        }
    }

    // 4. 拷贝赋值：先释放旧资源的计数，再接管新资源并 +1
    SimpleSharedPtr& operator=(const SimpleSharedPtr& other) noexcept {
        if (this != &other) {
            release_ref();
            ptr_ = other.ptr_;
            count_ = other.count_;
            if (count_) {
                ++(*count_);
            }
        }
        return *this;
    }

    // 5. 移动构造：直接窃取所有权，计数不变，原指针置空
    SimpleSharedPtr(SimpleSharedPtr&& other) noexcept
        : ptr_(other.ptr_), count_(other.count_) {
        other.ptr_ = nullptr;
        other.count_ = nullptr;
    }

    // 6. 移动赋值：释放旧资源，直接窃取新资源
    SimpleSharedPtr& operator=(SimpleSharedPtr&& other) noexcept {
        if (this != &other) {
            release_ref();
            ptr_ = other.ptr_;
            count_ = other.count_;
            other.ptr_ = nullptr;
            other.count_ = nullptr;
        }
        return *this;
    }

    // 常用接口
    T& operator*() const noexcept { return *ptr_; }
    T* operator->() const noexcept { return ptr_; }
    T* get() const noexcept { return ptr_; }
    size_t use_count() const noexcept { return count_ ? *count_ : 0; }

private:
    // 核心辅助函数：计数减 1，若归零则同时销毁数据与计数器
    void release_ref() noexcept {
        if (count_) {
            --(*count_);
            if (*count_ == 0) {
                delete ptr_;
                delete count_;
            }
        }
    }

    T* ptr_ = nullptr;        // 指向实际管理的对象
    size_t* count_ = nullptr; // 指向堆上的引用计数器
};

//--------------------------weak_ptr(不太重点)--------------------
//std::weak_ptr 核心用于解决 std::shared_ptr 的循环引用内存泄漏，
//以及在不拥有对象所有权的前提下安全观察对象是否存活。
// 在将当前对象 this 绑定到异步线程、定时器或网络回调（如 std::bind / Lambda）时：
// • 如果直接传裸指针 this，回调触发时对象可能已析构，导致未定义行为。
// • 如果传 shared_ptr，会强行延长对象生命周期，甚至导致对象销毁时机不可控。
// • 标准做法：让类继承 std::enable_shared_from_this<T>，在 Lambda 中捕获 weak_from_this()；
// 回调执行时先 auto self = weak_self.lock()，确认对象存活后再安全执行逻辑。


//---------------------------------------------重点 Socket编程过程---------------------------------------
/**
 * socket服务端编程流程（直接原画背）：首先通过socket函数（第一个参数AF_INET为IPV4协议，第二个参数放SOCK_STREAM表示
 * TCP协议，放SOCK_DGRAM表示UDP协议，第三个参数一般填0，让内核自己确定是TCP还是UDP），然后std::bind（）绑定sockaddr_arg 结构体，
 * 结构体中主要设置四个成员变量（地址组类型IPV4/IPV6，端口号，IP地址，保留字节一般填0），设置端口和地址要通过htonl函数小端转大端
 * 然后std::listen（）这里和客户端完成三次握手（第一个参数填fd，第二个参数nginx填511，高并发下直接填SOMAXCONNS）
 * listen这里socket就从客户端变为被动服务端，内核开始接受连接请求，然后accept() 函数的作用是从内核已经完成 TCP 三次握手的“全连接队列”中，取出一个客户端连接，
 * 并为其创建一个全新的专属通信套接字。后面就使用这个新的fd来read，write，最后是close。
 */

 //------------------------------------------多线程相关（进程与线程、互斥锁、原子操作、死锁、线程池）--------------------------
 /**
  * 线程VS进程：线程是操作系统分配资源的最小单位，进程是cpu调度的最小单位，一般高稳定和强隔离的情况使用多进程（例如Chrome开多个网页多网页之间要多进程）
  * 多线程用于需要频繁数据共享、高并发低开销切换的场景（例如服务器接受连接）还有个协程，主要用在高并发服务器中，C++20引入的新特性，如果不加协程的话容易频繁写回调函数
  * 
  * 互斥锁：保证同一时刻只有一个线程能访问共享资源，从而防止并发读写导致的数据错乱。我的项目中一般都用最普通的std::mutex锁，还有读写锁允许多个线程同时读，但是仅一个写（感觉能用到我的项目中，但是一直没用到）
  * RAII管理器的锁一般是lock_guard()和unique_lock(),后者可以手动开锁上锁，配合cv实现等待延迟加锁，前者用着方便但是不能手动unlock。
  * 
  * 原子操作（std::atomic）一般用于对单个简单变量（如整数、布尔值、指针）进行高频多线程读写、状态同步，我的项目中多次用到原子操作。
  * 线程池、server、主从react中running标志位由于被信号处理函数、accept 循环、工作线程同时访问，看到false的时间不确定，原子操作保证running=false之后
  * 的代码对所有线程可见，另外将running设置为原子操作对于程序编译也有好处，可以提高性能，防止编译器把杜操作提到循环外
  * 
  * 追问原子操作为什么不加锁：由于running一般都是在loop循环内判断如果加锁会带来很大的开锁解锁开销，原子操作没有开销，
  * 但是更致命的是在信号量下会死锁，如果一个线程池stop里面running加锁，读/写回调函数也加锁，现在又这样一个时序：stop持锁执行，信号打断进入读写
  * 读/写申请锁，这样stop持锁，read在等stop解锁，但是stop又在等read读完，互相等待直接死锁。
  * 
  * 死锁：上面就是一个例子，死锁在项目中也多次发生，死锁的通俗描述就是互等，例如A把车停到了B的车库内，B把车停到A的车库内，现在A，B想回到自己的车库但是都不先挪车互等
  * 我的项目中一个bug就是死锁导致的，client、server双端都用阻塞send，且都先写完再读，
  * 
  * 时刻   客户端                          服务端
    ──────────────────────────────────────────────────────────────
    t1    sendall(8MB) 开始               recv → 业务 → send 回去
    t2    client 发送缓冲满 → 阻塞          server 发送缓冲满 → 阻塞
    t3    停在 send，永不 read             停在 send，永不 recv
    t4    ┌────────────────────┐          ┌────────────────────┐
        │ 等 server 读走我的  │  ──需要──►│ 但它在 send 里卡住 │
        │ 数据，send 才能返回 │          │                    │
        └────────────────────┘          └────────────────────┘
                        ▲                            │
                        └─────────需要──────────────┘
                        client 读走它的数据，
                        server 的 send 才能返回
  * 
  * 
  */