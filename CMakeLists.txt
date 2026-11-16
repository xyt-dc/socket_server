cmake_minimum_required(VERSION 3.16)

project(socket LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
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
