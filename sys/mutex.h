#ifndef _YODB_MUTEX_H_
#define _YODB_MUTEX_H_

#include "sys/thread.h"
#include <pthread.h>
#include <boost/noncopyable.hpp>

namespace yodb {

class Mutex : boost::noncopyable {
public:
    explicit Mutex()
        : holder_(0)
    {
        assert(holder_ == 0);
        pthread_mutex_init(&mutex_, NULL);
    }

    ~Mutex() 
    {
        pthread_mutex_destroy(&mutex_);
    }

    void lock()
    {
        pthread_mutex_lock(&mutex_);
        holder_ = current_thread::get_tid();
    }

    void unlock()
    {
        holder_ = 0;
        pthread_mutex_unlock(&mutex_);
    }

    bool is_locked_by_this_thread() const
    {
        return holder_ == current_thread::get_tid();
    }

    pthread_mutex_t* mutex() { return &mutex_; }
private:
    pid_t holder_;
    pthread_mutex_t mutex_;
};

class ScopedMutex : boost::noncopyable {
public:
    explicit ScopedMutex(Mutex& mutex)
        : mutex_(mutex)
    {
        mutex_.lock();
    }

    ~ScopedMutex()
    {
        mutex_.unlock();
    }

private:
    Mutex& mutex_;
};

} // namespace yodb

#endif // _YODB_MUTEX_H_
