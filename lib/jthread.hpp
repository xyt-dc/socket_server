#pragma once

#include <thread>
#include <utility>
#include <type_traits>

class my_jthread {
public:
    my_jthread() noexcept = default;

    // 接受任意可调用对象及参数
    template <typename Callable, typename... Args>
    explicit my_jthread(Callable&& func, Args&&... args)
        : t(std::forward<Callable>(func), std::forward<Args>(args)...) {}

    // 仅支持 Move，不支持 Copy
    my_jthread(const my_jthread&) = delete;
    my_jthread& operator=(const my_jthread&) = delete;

    my_jthread(my_jthread&& other) noexcept : t(std::move(other.t)) {}

    my_jthread& operator=(my_jthread&& other) noexcept {
        if (this != &other) {
            if (joinable()) {
                join();
            }
            t = std::move(other.t);
        }
        return *this;
    }

    ~my_jthread() noexcept {
        if (joinable()) {
            join();
        }
    }

    void swap(my_jthread& other) noexcept {
        t.swap(other.t);
    }

    bool joinable() const noexcept {
        return t.joinable();
    }

    void join() {
        t.join();
    }

    void detach() {
        t.detach();
    }

    std::thread::id get_id() const noexcept {
        return t.get_id();
    }

    std::thread& native_handle() {
        return t;
    }

private:
    std::thread t;
};
