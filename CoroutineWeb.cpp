#include "CoroutineWeb.h"

CoroutineWeb::CoroutineWeb(int port) : _running(true), _port(port)
{
    init();
}

CoroutineWeb::~CoroutineWeb()
{
    close(_listen_fd);
    close(_ep_fd);
}

void CoroutineWeb::init()
{
    signal(SIGPIPE, SIG_IGN);
    _listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    // 设置端口复用，防止重启时 Address already in use
    int opt = 1;
    setsockopt(_listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    setsockopt(_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int flags = fcntl(_listen_fd, F_GETFL, 0);
    fcntl(_listen_fd, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(_port);

    bind(_listen_fd, (sockaddr*)&server_addr, sizeof(server_addr));
    listen(_listen_fd, SOMAXCONN);

    _ep_fd = epoll_create1(0);

    epoll_event event;
    event.events = EPOLLIN | EPOLLET | EPOLLEXCLUSIVE;
    event.data.fd = _listen_fd;
    epoll_ctl(_ep_fd, EPOLL_CTL_ADD, _listen_fd, &event);

    std::cout << "start listen...\n";
}

void CoroutineWeb::run()
{
    std::vector<epoll_event> events(1024);
    while (_running)
    {
        int nums = epoll_wait(_ep_fd, events.data(), 1024, -1);
        for (int i = 0; i < nums; ++i)
        {
            //注册的监听套接字有连接任务
            if (events[i].data.fd == _listen_fd)
            {
                //可能有多个连接请求
                while (true)
                {
                    sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int conn_fd = accept(_listen_fd, (sockaddr *)&client_addr, &client_len);
                    //处理完了
                    if (conn_fd < 0) {
                        break;
                    }
                    // 设置非阻塞
                    int flags = fcntl(conn_fd, F_GETFL, 0);
                    fcntl(conn_fd, F_SETFL, flags | O_NONBLOCK);

                    // 先把它加入 epoll，随便塞个 data，因为马上协程就会把它改掉
                    epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = conn_fd; 
                    epoll_ctl(_ep_fd, EPOLL_CTL_ADD, conn_fd, &ev);

                    add_coroutine_task(conn_fd);
                }
            }
            else
            {
                auto h = std::coroutine_handle<>::from_address(events[i].data.ptr);
                if (h)
                {
                    h.resume();
                }
            }
        }
    }
}

void CoroutineWeb::stop()
{
    _running = false;
}

Task CoroutineWeb::add_coroutine_task(int fd)
{
    char buffer[1024];

    while (true)
    {
        // 1. 读取客户端的 HTTP 请求（不关心具体内容和长度，只要能读出一点东西就行）
        int length = co_await AsyncRead{_ep_fd, fd, buffer, 1024};

        if (length <= 0) break;

        int write_pos = 0;

        // 3. 循环保证响应完全写回
        while (write_pos < length)
        {
            int w_len = co_await AsyncWrite{_ep_fd, fd, (char*)(buffer + write_pos), (ssize_t)(length - write_pos)};
            if (w_len <= 0) {
                close(fd);
                co_return;    // 直接结束协程！
            }
            write_pos += w_len;
        }
    }

    // 4. 断开连接并回收协程
    close(fd);
}
