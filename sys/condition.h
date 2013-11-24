#ifndef _YODB_COND_H_
#define _YODB_COND_H_

#include "sys/mutex.h"
#include <pthread.h>
#include <boost/noncopyable.hpp>

namespace yodb {

class CondVar : boost::noncopyable {
public:
    CondVar(Mutex& mutex)
        : mutex_(mutex)
    {
        pthread_cond_init(&cond_, NULL);
    }

    ~CondVar()          { pthread_cond_destroy(&cond_); }

    void wait()         { pthread_cond_wait(&cond_, mutex_.mutex()); }
    void notify()       { pthread_cond_signal(&cond_); }
    void notify_all()   { pthread_cond_broadcast(&cond_); }

private:
    Mutex& mutex_;
    pthread_cond_t cond_;
};

} // namespace yodb

#endif // _YODB_COND_H_
