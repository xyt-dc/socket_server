#pragma once

/**
 *  这个类我们实现的目的只有一个，就是让close（） 🈯️出现在析构函数里，走RAII的设计模式
 *  别的地方一律不close
 * 
 */

#include <iostream>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>


class Connection{
public:
    explicit Connection(int fd) noexcept:_fd(fd){}
    ~Connection(){
        if(_fd >= 0){
            ::close(_fd);
        }
    }

    /**
     *   资源管理：对于管理如动态分配的内存、文件句柄、网络连接等资源的类，禁止拷贝可以确保资源不会被意外复制，从而避免资源泄露、重复释放等问题。
     *   保证唯一性：某些对象设计为全局唯一或单例模式时，禁止拷贝可以确保整个程序中只有一个实例。
     *   避免浅拷贝问题：如果类包含指针成员，默认的拷贝构造函数和赋值运算符会导致浅拷贝，即拷贝的对象和原对象共享相同的指针成员，这可能引起数据不一致或资源释放错误。禁用它们可以促使开发者实现深拷贝或使用智能指针等技术来管理资源。
     *   提高代码安全性：通过禁用拷贝构造函数和赋值函数，可以减少因不当复制引起的错误和资源泄露问题。
     *   简化设计：在某些设计模式中，如单例模式或工厂模式，对象的拷贝是不被允许的，禁用这些函数可以使类的设计更加直观和简洁。
     *   增强类的封装性：通过控制对象的复制行为，可以更好地封装类的内部实现细节，保护对象的完整性。
     */
    Connection (const Connection&) = delete;
    Connection& operator = (const Connection&) = delete;

    // 读缓冲区：粘包/拆包用。长度前缀协议解析时，半条消息留在这里等下次。
    // 阶段 3 实现，现在留空。
    // std::vector<uint8_t> rbuf;

    // 写缓冲区：send 遇 EAGAIN 时数据存这里，等 EPOLLOUT 再冲刷。
    // ★ 这是 TLS 的硬前置条件，不能跳过。
    // 阶段 2 后半段实现。
    // std::vector<uint8_t> wbuf;

    int fd() const noexcept{return _fd;}
    void shutdown_rdwr() noexcept {
        if (_fd >= 0){
            ::shutdown(_fd,SHUT_RDWR);
        }
    }
private:
    int _fd;
};
