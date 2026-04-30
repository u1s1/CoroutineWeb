#pragma once
#include <coroutine>
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

struct Task
{
    struct promise_type
    {
        Task get_return_object() {return {std::coroutine_handle<promise_type>::from_promise(*this)};}
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {} // 如果协程不返回具体值
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> _h;
    Task():_h(nullptr){}
    Task(std::coroutine_handle<promise_type> h) :_h(h){}
    Task(const Task &) = delete;
    Task& operator=(const Task &) = delete;
    Task(Task&& t)noexcept : _h(t._h)
    {
        t._h = nullptr;
    }
    Task& operator=(Task&& t)noexcept
    {
        if (&t != this)
        {
            if (_h) 
            {
                _h.destroy();
            }
            _h = t._h;
            t._h = nullptr;
        }
        return *this;
    }
    ~Task()
    {
        if (_h) 
        {
            _h.destroy();
        }
    }
};

struct AsyncRead
{
    int epfd;
    int fd;
    char* buffer;
    ssize_t length;
    ssize_t result = 0;
    bool await_ready() {
        result = read(fd, buffer, length);
        if (result >= 0)
        {
            return true;
        }
        
        //没读到数据或者读完了，挂起
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return false;
        }

        return true;
    }

    void await_suspend(std::coroutine_handle<> handle){
        epoll_event event;
        event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        event.data.ptr = handle.address();
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event);
    }

    ssize_t await_resume()
    {
        std::cout << "read resume\n";
        //如果已经读成功或者确定失败
        if (result >= 0 || (result < 0 && errno != EAGAIN))
        {
            return result;
        }
        return read(fd, buffer, length);
    }
};

struct AsyncWrite
{
    int epfd;
    int fd;
    char* buffer;
    ssize_t length;
    ssize_t result = 0;
    bool await_ready() {
        result = write(fd, buffer, length);
        if (result >= 0)
        {
            return true;
        }
        
        //没写到数据或者写完了，挂起
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return false;
        }

        return true;
    }

    void await_suspend(std::coroutine_handle<> handle){
        epoll_event event;
        event.events = EPOLLOUT | EPOLLET | EPOLLONESHOT;
        event.data.ptr = handle.address();
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event);
    }

    ssize_t await_resume()
    {
        std::cout << "write resume\n";
        //如果已经写成功或者确定失败
        if (result >= 0 || (result < 0 && errno != EAGAIN))
        {
            return result;
        }

        return write(fd, buffer, length);
    }
};