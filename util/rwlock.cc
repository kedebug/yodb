#include "util/rwlock.h"

using namespace yodb;

RWLock::RWLock()
    : mutex_(), 
      cond_wait_read_(mutex_), 
      cond_wait_write_(mutex_),
      readers_(0), 
      writers_(0), 
      want_readers_(0), 
      want_writers_(0) 
{
}

void RWLock::read_lock()
{
    ScopedMutex lock(mutex_);

    if (writers_ || want_writers_) {
        ++want_readers_;

        while (writers_ || want_writers_)
            cond_wait_read_.wait();

        // it's our turn now
        assert(writers_ == 0);
        assert(want_readers_ > 0);

        --want_readers_;
    }
    ++readers_;
}

void RWLock::read_unlock()
{
    ScopedMutex lock(mutex_);

    assert(readers_ > 0);
    assert(writers_ == 0);

    --readers_;

    if (readers_ == 0 && want_writers_)
        cond_wait_write_.notify();
}

void RWLock::write_lock()
{
    ScopedMutex lock(mutex_);
    
    if (readers_ || writers_) {
        ++want_writers_;
        
        while (readers_ || writers_)
            cond_wait_write_.wait();

        // it's our turn now
        assert(readers_ == 0);
        assert(want_writers_ > 0);

        --want_writers_;
    }
    ++writers_;
}

void RWLock::write_unlock()
{
    ScopedMutex lock(mutex_);

    assert(readers_ == 0);
    assert(writers_ == 1);

    --writers_;

    // writer first
    if (want_writers_)
        cond_wait_write_.notify();
    else if (want_readers_)
        cond_wait_read_.notify_all();
}

