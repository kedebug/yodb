#include "tree/msg.h"

using namespace yodb;

size_t MsgBuf::msg_count()
{
    ScopedMutex lock(mutex_);
    return msgbuf_.size();
}

void MsgBuf::release()
{
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
    assert(mutex_.is_locked_by_this_thread());
    msgbuf_.resize(size);
}

MsgBuf::Iterator MsgBuf::find(Slice key)
{
    assert(mutex_.is_locked_by_this_thread());

    Iterator iter = begin();
    for (; iter != end(); iter++) {
        if (comparator_->compare(key, iter->key()) == 0)
            break;
    }
    return iter;

    // if (msgbuf_.empty()) return end();

    // int left = 0, right = msgbuf_.size() - 1; 

    // while (left <= right) {
    //     int middle = (right + left) / 2;
    //     int comp = comparator_->compare(key, msgbuf_[middle].key());

    //     if (comp > 0)
    //         left = middle + 1;
    //     else if (comp < 0)
    //         right = middle - 1;
    //     else
    //         return begin() + middle;
    // }

    // return end(); 
}
