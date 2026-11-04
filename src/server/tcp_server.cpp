#include "tcp_server.hpp"
#include <netinet/in.h>
#include <cstring>
#include <functional>
#include <unistd.h>


void TcpServerBase::create_socket(){
    _listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (_listen_fd<0){
        std::cerr << "socket error" << std::endl;
    }
    std::cout<<"socket success"<<std::endl;
}

void TcpServerBase::set_reuse_addr(){
    int optval = 1; //SO_REUSEADDR 期望读一个 int，Linux 要求 optlen >= sizeof(int)，传 bool(1字节) 会报 EINVAL
    int opt = setsockopt(_listen_fd,SOL_SOCKET,SO_REUSEADDR,&optval,sizeof(optval));//解决程序关闭重启时报“address already in use
    if(opt <0 )
        perror("port reuse failed");
}

void TcpServerBase::do_bind(){
    struct sockaddr_in server_addr;
    
    /**

    sin_family：地址族类型。对于 IPv4 网络通信，该值必须设置为 AF_INET。
    sin_port：传输层端口号（16位，2字节）。必须使用 htons() 函数从主机字节序转换为网络字节序（大端序）。
    sin_addr：包含一个 in_addr 结构体，内部的 s_addr 表示 32 位（4字节）的 IPv4 地址，htonl(),同样需要使用网络字节序。
    sin_zero：保留字节，通常使用 memset() 将其全部清零（0），用于对齐通用结构体 sockaddr。

*/
    memset(&server_addr,0,sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(_port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    int res = bind(_listen_fd,(const struct sockaddr*) &server_addr,sizeof(server_addr));
    if (res <0)
        std::cout<<"bind failed"<<std::endl;
}

void TcpServerBase::do_listen(){
    int listen_res = listen(_listen_fd, 5);
    if (listen_res < 0)
        std::cout<<"listen_failed"<<std::endl; 
}

int TcpServerBase::accept_once(struct sockaddr_in &server_addr){
    socklen_t len = sizeof(server_addr);
    //不对在于这里异常一抛我的那套错误状态机根本没机会跑后面就全乱了
    // int accept_res = accept(_listen_fd, (struct sockaddr *)&server_addr,&len);
    // if(accept_res < 0) {
    //     throw std::runtime_error("accept failed ");
    // }
    // return accept_res;
    return accept(_listen_fd,(struct sockaddr *)&server_addr,&len);
}
void TcpServerBase::handle_client_read(int client_fd){
    char buf[1024] = {0};
    ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
    if(n>0){
        std::cout.write(buf, n); 
        std::cout << std::endl;
    }else if(n == 0){
        std::cout<<"client close"<<std::endl;
    }else{
        perror("recv error");
    }
}