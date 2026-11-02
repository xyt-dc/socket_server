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
