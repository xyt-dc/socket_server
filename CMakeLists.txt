cmake_minimum_required(VERSION 3.16)

project(socket LANGUAGES CXX)

# ---------------------------------------------------------------------------
# C++ 标准
#
# 为什么是 20：执行层异步化要用 <coroutine>（co_await / promise_type）。
# 注意：协程不是「标准库给了就能用」，它需要编译器打开协程支持。
# GCC 11+ 在 -std=c++20 下默认开启，但用 gnu++20 或显式 -fcoroutines 更稳。
#
# ★ 这里的 set() 会【覆盖】命令行 -DCMAKE_CXX_STANDARD=xx 的值，
#   所以上面不能再让用户从外部指定 —— 想调整请改这一行。
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)   # 用 gnu++20：保留 POSIX 扩展（accept4/eventfd 声明更省事）
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(socket_server
    src/main.cpp
    src/server/tcp_server.cpp
    src/server/tcp_server.hpp
)

target_include_directories(socket_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
)

# std::thread 需要链接 pthread
find_package(Threads REQUIRED)
target_link_libraries(socket_server PRIVATE Threads::Threads)

#json 需要链接nlohmann_json
find_package(nlohmann_json 3.11 REQUIRED)          # 放在文件靠前位置
target_link_libraries(socket_server PRIVATE Threads::Threads nlohmann_json::nlohmann_json)

# 压测专用入口：支持 --server=thread|pool 运行时切换模型
# 单独一个 target，src/main.cpp 保持原样不动
add_executable(bench_server
    test/net/bench_server.cpp
    src/server/tcp_server.cpp
    src/server/tcp_server.hpp
)

target_include_directories(bench_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
)

target_link_libraries(bench_server PRIVATE Threads::Threads)

# ---------------------------------------------------------------------------
# 单元测试（之前是 test/suites/*.sh 里手写 g++ 编译，现在同时注册成 CMake target）
#
# ★ 为什么要注册成 target：
#   只有这样 IDE 才能"看见"它们。VS Code 装了 CMake Tools 插件后，
#   每个 target 自动变成可调试目标（相当于 CLion 的体验），
#   不用为每个文件手写 launch.json 配置。
#
# ★ 为什么同时保留 test/suites/*.sh 里的手写 g++：
#   脚本要跑 -Wall -Wextra -Werror 的严格检查，还要在不装 cmake 的
#   环境里能跑。两条路互不干扰，产物同名但不在同一目录。
#
# ★ 为什么用 -O0 -g：
#   这些库全是头文件里的 inline 函数。项目默认无 -g 且若开优化，
#   函数会被内联进调用方，IDE 里打的断点会静默失效。
# ---------------------------------------------------------------------------
set(UNIT_TEST_SOURCES
    test/unit/blocking_queue_test.cpp
    test/unit/frame_test.cpp
    test/unit/thread_pool_test.cpp
    test/unit/timer_test.cpp
    test/unit/idle_scanner_test.cpp
)

foreach(unit_src IN LISTS UNIT_TEST_SOURCES)
    get_filename_component(unit_name ${unit_src} NAME_WE)
    add_executable(${unit_name} ${unit_src})
    target_include_directories(${unit_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/lib
        ${CMAKE_CURRENT_SOURCE_DIR}/src/timer
    )
    target_link_libraries(${unit_name} PRIVATE Threads::Threads)
    # 调试友好：无优化 + 调试符号。单测都很快，不需要 -O2
    target_compile_options(${unit_name} PRIVATE -g -O0)
endforeach()
