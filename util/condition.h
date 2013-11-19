#ifndef _YODB_COND_H_
#define _YODB_COND_H_

#include "util/mutex.h"
#include <pthread.h>
#include <boost/noncopyable.hpp>

namespace yodb {

class CondVar : boost::noncopyable {
public:
    CondVar(Mutex& mutex, bool static_cond = false)
        : mutex_(mutex), static_(static_cond)
    {
        if (static_)
            cond_ = PTHREAD_COND_INITIALIZER;
        else
            pthread_cond_init(&cond_, NULL);
    }

    ~CondVar()
    {
        if (!static_)  
            pthread_cond_destroy(&cond_);
    }

    void wait()         { pthread_cond_wait(&cond_, mutex_.mutex()); }
    void notify()       { pthread_cond_signal(&cond_); }
    void notify_all()   { pthread_cond_broadcast(&cond_); }

private:
    Mutex& mutex_;
    pthread_cond_t cond_;
    bool static_;
};

} // namespace yodb

#endif // _YODB_COND_H_
