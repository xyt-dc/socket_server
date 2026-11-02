//#include <asm-generic/socket.h>
//#include <cstdio>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <cstring>
#include "tcp_server.hpp"

/**
server -> socket() -> bind() ->listen() ->accept() -> recv() ->close()
client -> socket() ->connect() ->send() ->close()

*/


// int main(){
//     int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
//     if (listen_fd<0){
//         std::cerr << "socket error" << std::endl;
//         return -1;
//     }

//     std::cout<<"socket success"<<std::endl;
    
//     /**
//     int setsockopt( int socket, int level, int option_name,const void *option_value, size_t ，ption_len);
//     第一个参数socket是套接字描述符。
//     第二个参数level是被设置的选项的级别，想要在套接字上设置选项就必须是SOL_SOCKET
//     option_name指定准备设置的选项: SO_DEBUG，打开或关闭调试信息。 SO_REUSEADDR，打开或关闭地址复用功能。SO_DONTROUTE，打开或关闭路由查找功能。SO_BROADCAST，允许或禁止发送广播数据。
//         SO_SNDBUF，设置发送缓冲区的大小。 SO_RCVBUF，设置接收缓冲区的大小。 SO_KEEPALIVE，套接字保活。SO_OOBINLINE，紧急数据放入普通数据流。SO_NO_CHECK，打开或关闭校验和。
//     https://www.cnblogs.com/cthon/p/9270778.html
//     */
//     int optval = 1; //SO_REUSEADDR 期望读一个 int，Linux 要求 optlen >= sizeof(int)，传 bool(1字节) 会报 EINVAL
//     int opt = setsockopt(listen_fd,SOL_SOCKET,SO_REUSEADDR,&optval,sizeof(optval));//解决程序关闭重启时报“address already in use
//     if(opt <0 )
//         perror("port reuse failed");
//     struct sockaddr_in server_addr;
    
//     /**

//     sin_family：地址族类型。对于 IPv4 网络通信，该值必须设置为 AF_INET。
//     sin_port：传输层端口号（16位，2字节）。必须使用 htons() 函数从主机字节序转换为网络字节序（大端序）。
//     sin_addr：包含一个 in_addr 结构体，内部的 s_addr 表示 32 位（4字节）的 IPv4 地址，htonl(),同样需要使用网络字节序。
//     sin_zero：保留字节，通常使用 memset() 将其全部清零（0），用于对齐通用结构体 sockaddr。

// */
//     memset(&server_addr,0,sizeof(server_addr));
//     server_addr.sin_family = AF_INET;
//     server_addr.sin_port = htons(3006);
//     server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
//     /**
//         int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen);
//         socket文件描述符  addr:构造出IP地址加端口号  addrlen:sizeof(addr)长度
//     */
// // ::bind 中的 :: 是作用域解析运算符（scope resolution operator）
// // ::bind 表示调用全局命名空间中的 bind 函数，而非 std::bind 或其他命名空间中的 bind
// // 这在避免命名冲突时很有用，因为 C++11 在 <functional> 中引入了 std::bind
// // 这里直接调用 ::bind 是为了明确使用 C 标准库的系统调用 bind
// // 如果没有 using namespace std 或者其他同名 bind 在作用域中，直接写 bind 也可以
// // bind(listen_fd,&server_addr,sizeof(server_addr)); // 这种写法也没问题
//     int res = bind(listen_fd,(const struct sockaddr*) &server_addr,sizeof(server_addr));
//     if (res <0)
//         std::cout<<"bind failed"<<std::endl;

//     /**
//         int listen(int sockfd, int backlog);第一个参数即为要监听的socket描述字，第二个参数为相应socket可以排队的最大连接个数。
//         int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen);
//         第一个参数即为客户端的socket描述字，第二参数为服务器的socket地址，第三个参数为socket地址的长度。
//     */
//     int listen_res = listen(listen_fd, 5);
//     if (listen_res < 0)
//         std::cout<<"listen_failed"<<std::endl; 

//     /**
//         extern int accept (int __fd, __SOCKADDR_ARG __addr,
// 		   socklen_t *__restrict __addr_len);
//     */
//     socklen_t len = sizeof(server_addr);
//     int accept_res = accept(listen_fd, (struct sockaddr *)&server_addr,&len);
//     if(accept_res < 0) {
//         perror("can't bind local address");
//      }

//     while (true) {
//         char buf[1024] = {0};
//         /**
//             • > 0：成功接收并复制到应用层缓冲区的字节数。
//             • == 0：连接已正常关闭（对方执行了有序关闭/EOF）。
//             • < 0（Linux下为 -1，Windows下为 SOCKET_ERROR）：发生错误，具体错误码可通过 errno（Linux）或 WSAGetLastError()（Windows）查看。
        
//         */
//         ssize_t n = recv(accept_res, buf, sizeof(buf), 0);
//         if(n>0){
//             std::cout.write(buf, n); 
//             std::cout << std::endl;
//             continue;
//         }else if(n == 0){
//             std::cout<<"client close"<<std::endl;
//         }else{
//             perror("recv error");
//         }
//         break;
//      }
    

//     close(listen_fd);
//     return 0;
// }