#include "tree/msg.h"

using namespace yodb;

size_t MsgBuf::msg_count()
{
    ScopedMutex lock(mutex_);
    return msgbuf_.size();
}

void MsgBuf::release()
{
    ScopedMutex lock(mutex_);
    msgbuf_.clear();
}

void MsgBuf::insert(const Msg& msg)
{
    ScopedMutex lock(mutex_);

    Iterator iter = begin();

    for (; iter != end(); iter++) {
        if (comparator_->compare(msg.key(), iter->key()) <= 0)
            break;
    }
    if (iter == end() || iter->key() != msg.key()) {
        msgbuf_.insert(iter, msg);
    } else {
        iter->release();
        *iter = msg;
    }
}

void MsgBuf::insert(Iterator pos, Iterator first, Iterator last)
{
    ScopedMutex lock(mutex_);
    msgbuf_.insert(pos, first, last); 
}

void MsgBuf::resize(size_t size)
{
    ScopedMutex lock(mutex_);
    msgbuf_.resize(size);
}

MsgBuf::Iterator MsgBuf::find(Slice key)
{
    assert(mutex_.is_locked_by_this_thread());

    size_t left = 0, right = msgbuf_.size() - 1; 

    while (left <= right) {
        size_t middle = (right + left) / 2;
        int comp = comparator_->compare(key, msgbuf_[middle].key());

        if (comp > 0)
            middle = right - 1;
        else if (comp < 0)
            middle = left + 1;
        else
            return begin() + middle;
    }

    return end(); 
}
