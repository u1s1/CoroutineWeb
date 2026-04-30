#pragma once
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <thread>
#include <cstring>
#include <cerrno>
#include <stdio.h>
#include <unordered_map>
#include "AsyncReadWrite.h"

class CoroutineWeb
{
public:
    CoroutineWeb(int port);
    ~CoroutineWeb();

    void init();

    void run();

    void stop();

    Task add_coroutine_task(int fd);
private:
    int _port;
    int _listen_fd;
    int _ep_fd;
    bool _running;
};