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
    web_head head;
    int head_size = sizeof(head);

    while (true)
    {
        int ready_size = 0;
        int temp_size = 0;

        //收取数据头
        while (ready_size < head_size)
        {
            temp_size = co_await AsyncRead{_ep_fd, fd, (char *)(((char*)&head) + ready_size),
                                            (ssize_t)(head_size - ready_size)};
            if (temp_size <= 0)
            {
                close(fd);
                co_return;    // 直接结束协程！
            }
            ready_size += temp_size;
        }
        if (head.length == 0 || head.length > 1024)
        {
            close(fd);
            co_return; 
        }
        //收取数据体
        ready_size = 0;
        while (ready_size < head.length)
        {
            temp_size = co_await AsyncRead{_ep_fd, fd, (char *)(buffer + ready_size),
                                                     (ssize_t)(head.length - ready_size)};
            if (temp_size <= 0)
            {
                close(fd);
                co_return;    // 直接结束协程！
            }
            ready_size += temp_size;
        }

        //未来在此处添加数据处理步骤

        //发送数据头
        ready_size = 0;
        while (ready_size < head_size)
        {
            temp_size = co_await AsyncWrite{_ep_fd, fd, (char *)((char*)(&head) + ready_size),
                                            (ssize_t)(head_size - ready_size)};
            if (temp_size <= 0)
            {
                close(fd);
                co_return;    // 直接结束协程！
            }
            ready_size += temp_size;
        }
        //发送数据体
        ready_size = 0;
        while (ready_size < head.length)
        {
            temp_size = co_await AsyncWrite{_ep_fd, fd, (char *)(buffer + ready_size),
                                            (ssize_t)(head.length - ready_size)};
            if (temp_size <= 0)
            {
                close(fd);
                co_return;    // 直接结束协程！
            }
            ready_size += temp_size;
        }
    }

    // 4. 断开连接并回收协程
    close(fd);
}
