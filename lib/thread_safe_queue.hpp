#pragma once

#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>

template<typename T>
class Thread_safe_queue{
private:
    mutable std::mutex _mtx;
    std::queue<T> _data_queue;
    std::condition_variable _data_cond;
public:
    explicit Thread_safe_queue(){}
    //下面这个拷贝构造函数直接重写赋值构造函数多好，本质上也是在赋值
    explicit Thread_safe_queue(const Thread_safe_queue& queue){
        std::lock_guard<std::mutex> lk(queue._mtx);
        _data_queue = queue._mtx;
    }
    Thread_safe_queue operator = (const Thread_safe_queue& queue){
        std::lock_guard<std::mutex> lk(queue._mtx);
        _data_queue = queue._data_queue;
    }
    void push(const T& item){
        std::lock_guard<std::mutex> lk(_mtx);
        _data_queue.push_back(item);
        //productor
        _data_cond.notify_one();
    }

    //front & pop 易产生条件竞争所以说线程队列里不能将它俩拆开
    //详细说明看多线程实战第三章
    void pop(T& tmp){       
        //传一个引用
        std::unique_lock <std::mutex> lk(_mtx);
        
        _data_cond.wait(lk,[&](){
            return !_data_queue.empty();
        });
        tmp = _data_queue.front();
        _data_queue.pop();
        lk.unlock();
    }

    std::shared_ptr<T> pop(){
        std::unique_lock<std::mutex> lk (_mtx);
        _data_cond.wait(lk,[&](){
            return ! _data_queue.empty();
        });
        std::unique_ptr<T> val(std::make_shared<T>(_data_queue.front()));
        _data_queue.pop();
        return val;
    }
    

    bool empty()const{
        //用加锁吗？ 我想是要加锁，因为如果你两个线程一个在问是否empty
        //一个push，那么可能会出错，但是不知道这是不是竞争
        std::lock_guard<std::mutex> lk (_mtx);
        return _data_queue.empty();
    }
}