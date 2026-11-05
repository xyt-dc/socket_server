/**
    消息队列实现，独立公用的
    现在暂时先实现一个简单的MQ
    以后可以参考https://developer.aliyun.com/article/1140571来自己设计

    ===================== 以后可以优化的方向 =====================
    1. 环形缓冲区替代 deque：vector + head/tail/count，吞吐数倍
    2. 批量接口 push_bulk/pop_bulk：锁获取次数 N -> N/batch
    3. 超时版 try_push_for/try_pop_for：服务端最实用，队列满时
       宁可拒绝新连接，也不要卡住 accept 线程
    4. 统计指标：阻塞次数、等待总时长、队列水位线，
       —— 这是判断容量设得对不对的唯一依据
    5. 关闭状态细分 Stopping/Stopped：停机中仍允许 push
       （收在途连接），只是不再 accept 新连接
    6. 无锁化 Vyukov/moodycamel MPMC：正确性极难验证，现阶段别碰
    ============================================================
*/

#pragma once
#include <deque>
#include <mutex>
#include <condition_variable>
#include <cstddef>
#include <stdexcept>

template<typename T>
class BlockingQueue{
public:
    //explicit 显式构造函数，比如说 
    /**
    class student{
        student(); 默认构造函数
        student(std::string name):_name(name){} 单参数构造函数
        ～student();
    };
    int main(){
        student t("tt"); ✅
        student x; 默认构造✅
        stident xx = “xx”✅但是这是隐式构造，student类型等于string类型，后面如果有错很难排查
    }
    */
    explicit BlockingQueue(size_t cap = 1024):_cap(cap){
        // cap==0 时 push 的谓词 size()<_cap 恒为假，且 _closed 也是 false，
        // 于是永久阻塞 -> 死锁。必须在构造期就挡住。
        // 注意 size_t 是无符号的，cap<=0 实际只等价于 cap==0，写成 ==0 更准确。
        // 这里选择抛异常而不是兜底成 1024：静默改掉用户传的值更难排查。
        if (cap == 0){
            throw std::invalid_argument("BlockingQueue: cap must be > 0");
        }
    }

    //productor 阻塞版
    bool push(T item){
        std::unique_lock<std::mutex> lk (_mtx);
        //[&] 表示以引用的方式处理所有外部变量,简单说【】就是捕获的意思，函数需要的东西就写在捕获里
        //【&】【=】指的是变量，如果捕获类的对象的话就要用【this】或【类名】
        /**
        wait()会去检查这些条件(通过调用所提供的lambda函数)，当条件满足(lambda函数返回true)
        时返回。如果条件不满足(lambda函数返回false)，wait()函数将解锁互斥量，并且将这个线程
        (上段提到的处理数据的线程)置于阻塞或等待状态。
        */
        _not_full.wait(lk,[&](){return _queue.size()<_cap||_closed;});

        if(_closed)return false;

        _queue.push_back(std::move(item));
        lk.unlock();
        _not_empty.notify_one();
        return true;
    }
    
    // 非阻塞版 push：满了立刻返回 false，绝不睡眠
    bool try_push(T item){
        std::unique_lock<std::mutex> lk(_mtx);
        if (_queue.size() >= _cap) return false;   // 满 → 立刻走人
        if (_closed) return false;
        _queue.push_back(std::move(item));
        lk.unlock();
        _not_empty.notify_one();
        return true;
    }

    //customer
    bool pop(T& out){
         std::unique_lock<std::mutex> lk(_mtx);
         //阻塞等待直到返回true：queue不空，或closed
         _not_empty.wait(lk,[&](){return !_queue.empty()||_closed;});
         if (_queue.empty()){
            //队列空了说明走的是closed条件跳过阻塞，说明队列传输已经关闭了直接返回false
            return false;
         }
         //这里得引用不能复制？但是我不知道为什么不能复制变量然后pop掉
         //解答：这里考虑到范型因为如果是线程或者unique_str的话不能复制，另外性能上复制要消耗性能，但是move只是移动指针整个过程零分配
         out = std::move(_queue.front());
         _queue.pop_front();
         lk.unlock();
         _not_full.notify_one();
         return true;
    }

    bool try_pop(T& out){
        std::unique_lock<std::mutex> lk(_mtx);
        if (_queue.empty()){
            return false;
        }
        out = std::move(_queue.front());
        _queue.pop_front();
        lk.unlock();
        _not_full.notify_one();
        return true;
    }

    //关闭连接时，置标志位+叫醒所有等待者
    void close(){
        {
            //如果这里不扩域的话，后面被唤醒的线程会抢锁，但是发现还锁着呢，➡️睡回去了，再回来发现已经置位放锁了
            //能跑但是多跑一轮上下文
            //扩域的话这里域走完锁就释放了
            std::unique_lock<std::mutex> lk (_mtx);
            _closed = true;
        }
        _not_empty.notify_all();
        _not_full.notify_all();
    }

    size_t size() const{
        std::lock_guard<std::mutex> lk(_mtx);
        return _queue.size();
    }

    bool closed() const{
        std::lock_guard<std::mutex> lk (_mtx);
        return _closed;
    }

    // 容量构造后不变，所以是 const 成员，不用加锁
    size_t capacity() const{ return _cap; }

private:
    std::deque<T> _queue;
    /**
        size_t简单理解为unsigned int就好了，sizet移植性强
        const：构造后不再变化，所以 capacity() 不用加锁
    */
    const size_t _cap;
    //mutable 弱化版的const，void fnctuon（）const 这里函数内部的数据是不允许更改的，如果使用mutable就可以
    //举例就是如果实现一个计数器，由于不能通过函数更改内部的count变量，所以函数后面要加入const，又因为count要计数所以
    //加一个mutable就可以解决了。
    //mutable使用时的注意事项. mutable在修饰变量时，只能修饰类的成员变量，不能修饰普通变量，否编译报错（non-member 'ret' cannot be declared 'mutable'）; 
    //同样const const函数声明末尾的修饰符适用于成员函数的隐藏参数this。由于一个自由函数没有this，因此不需要this该修饰符。
    mutable std::mutex _mtx;
    std::condition_variable _not_empty;
    std::condition_variable _not_full;
    bool _closed = false;


};