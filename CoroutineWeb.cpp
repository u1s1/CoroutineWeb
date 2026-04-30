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
    _listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    // 设置端口复用，防止重启时 Address already in use
    int opt = 1;
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
        std::cout << "new epoll_wait\n";
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

                    _coroutine_map.emplace(conn_fd, add_coroutine_task(conn_fd));
                }
            }
            else
            {
                auto h = std::coroutine_handle<>::from_address(events[i].data.ptr);
                if (h &&!h.done())
                {
                    h.resume();
                }
            }
        }
        for (int fd : _delete_fd)
        {
            _coroutine_map.erase(fd);
        }
        _delete_fd.clear();
    }
}

void CoroutineWeb::stop()
{
    _running = false;
}

Task CoroutineWeb::add_coroutine_task(int fd)
{
    std::vector<char> buffer(1024);
    int pos = 0;
    while (true)
    {
        int length = co_await AsyncRead{_ep_fd, fd, buffer.data() + pos, (ssize_t)1024 - pos};
        if (length <= 0)
        {
            break;
        }
        pos += length;
        if (pos < 1024)
        {
            continue;
        }

        pos = 0;
        while (true)
        {
            length = co_await AsyncWrite{_ep_fd, fd, buffer.data() + pos, (ssize_t)1024 - pos};
            if (length <= 0)
            {
                break;
            }
            pos += length;
            if (pos < 1024)
            {
                continue;
            }
            break;
        }
        break;
    }
    close(fd);
    _delete_fd.push_back(fd);
}
