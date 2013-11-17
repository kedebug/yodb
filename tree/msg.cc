#include "tree/msg.h"

using namespace yodb;

void MsgBuf::release()
{
    msgbuf_.clear();
}

void MsgBuf::insert(const Msg& msg)
{
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
    msgbuf_.insert(pos, first, last); 
}

MsgBuf::Iterator MsgBuf::find(Slice key)
{
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
