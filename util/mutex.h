#ifndef _YODB_MUTEX_H_
#define _YODB_MUTEX_H_

#include <pthread.h>
#include <boost/noncopyable.hpp>

namespace yodb {

class Mutex : boost::noncopyable {
public:
    explicit Mutex()
        : locked_(false)
    {
        pthread_mutex_init(&mutex_, NULL);
    }

    void lock()
    {
        pthread_mutex_lock(&mutex_);
        locked_ = true;
    }

    void unlock()
    {
        locked_ = false;
        pthread_mutex_unlock(&mutex_);
    }

    bool assert_locked()
    {
        return locked_;
    }

    pthread_mutex_t* get_mutex()
    {
        return &mutex_;
    }

    ~Mutex() 
    {
        pthread_mutex_destroy(&mutex_);
    }

private:
    bool locked_;
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
