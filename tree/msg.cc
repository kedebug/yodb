#include "tree/msg.h"

using namespace yodb;

MsgBuf::MsgBuf(Comparator* comparator)
    : msgbuf_(), comparator_(comparator), mutex_(), size_(0)
{
}

MsgBuf::~MsgBuf()
{
    for (Iterator iter = msgbuf_.begin(); iter != msgbuf_.end(); iter++)
        iter->release();
    msgbuf_.clear();
}

size_t MsgBuf::msg_count()
{
    ScopedMutex lock(mutex_);
    return msgbuf_.size();
}

size_t MsgBuf::size()
{
    return 4 + size_;
}

void MsgBuf::release()
{
    msgbuf_.clear();
    size_ = 0;
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
        size_ -= iter->size();
        iter->release();
        *iter = msg;
    }

    size_ += msg.size();
}

void MsgBuf::insert(Iterator pos, Iterator first, Iterator last)
{
    ScopedMutex lock(mutex_);
    msgbuf_.insert(pos, first, last); 
    
    for (Iterator iter = first; iter != last; iter++)
        size_ += iter->size();
}

void MsgBuf::resize(size_t size)
{
    assert(mutex_.is_locked_by_this_thread());
    msgbuf_.resize(size);

    size_ = 0;
    for (Iterator iter = msgbuf_.begin(); iter != msgbuf_.end(); iter++)
        size_ += iter->size();
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

bool MsgBuf::constrcutor(BlockReader& reader)
{
    assert(reader.ok());

    uint32_t count = 0;
    reader >> count;
    
    if (count == 0) return true;

    msgbuf_.reserve(count);

    for (size_t i = 0; i < count; i++) {
        uint8_t type;
        Slice key, value;

        reader >> type >> key;
        if (type == Put)
            reader >> value;

        msgbuf_.push_back(Msg((MsgType)type, key, value));
        size_ += msgbuf_.back().size();
    }

    return reader.ok();
}

bool MsgBuf::destructor(BlockWriter& writer)
{
    assert(writer.ok());

    uint32_t count = msgbuf_.size();
    writer << count;
    
    for (size_t i = 0; i < count; i++) {
        Msg msg = msgbuf_[i];
        uint8_t type = msg.type();

        writer << type << msg.key();
        if (type == Put)
            writer << msg.value();
    }
    
    return writer.ok();
}
