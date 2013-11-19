#ifndef _YODB_RWLOCK_H_
#define _YODB_RWLOCK_H_

#include "util/mutex.h"
#include "util/condition.h"
#include <boost/noncopyable.hpp>

namespace yodb {

class RWLock : boost::noncopyable {
public:
    RWLock();

    void read_lock();
    void read_unlock();

    void write_lock();
    void write_unlock();

private:
    Mutex mutex_;
    CondVar cond_wait_read_;
    CondVar cond_wait_write_;

    size_t readers_;
    size_t writers_;
    size_t want_readers_;
    size_t want_writers_;
};

} // namespace yodb

#endif // _YODB_RWLOCK_H_
