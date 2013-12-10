#include "tree/msg.h"

using namespace yodb;

MsgBuf::MsgBuf(Comparator* comparator)
    : list_(Compare(comparator)), 
      comparator_(comparator), 
      mutex_(), size_(0)
{
}

MsgBuf::~MsgBuf()
{
    Iterator iter(&list_);

    iter.seek_to_first();

    while (iter.valid()) {
        iter.key().release();
        iter.next();
    }

    list_.clear();
}

size_t MsgBuf::count()
{
    ScopedMutex lock(mutex_);
    return list_.count();
}

size_t MsgBuf::size()
{
    return 4 + size_;
}

void MsgBuf::clear()
{
    list_.clear();
    size_ = 0;
}

void MsgBuf::insert(const Msg& msg)
{
    ScopedMutex lock(mutex_);

    Iterator iter(&list_);
    iter.seek(msg);

    if (iter.valid()) {
        Msg got = iter.key();

        if (comparator_->compare(got.key(), msg.key()) == 0) {
            size_ -= got.size();
            got.release();
        }
    }

    list_.insert(msg);
    size_ += msg.size();
}

void MsgBuf::resize(size_t size)
{
    assert(mutex_.is_locked_by_this_thread());
    list_.resize(size);

    size_ = 0;
    Iterator iter(&list_);
    iter.seek_to_first();

    while (iter.valid()) {
        size_ += iter.key().size(); 
    }
}

bool MsgBuf::find(Slice key, Msg& msg)
{
    assert(mutex_.is_locked_by_this_thread());
    
    Msg fake(_Nop, key);
    Iterator iter(&list_);

    iter.seek(fake);
    
    if (iter.valid()) {
        msg = iter.key();
        return true;
    }

    return false;
}

bool MsgBuf::constrcutor(BlockReader& reader)
{
    assert(reader.ok());

    uint32_t count = 0;
    reader >> count;
    
    if (count == 0) return true;

    for (size_t i = 0; i < count; i++) {
        uint8_t type;
        Slice key, value;

        reader >> type >> key;
        if (type == Put)
            reader >> value;

        Msg msg((MsgType)type, key, value);
        list_.insert(msg);
        size_ += msg.size();
    }

    return reader.ok();
}

bool MsgBuf::destructor(BlockWriter& writer)
{
    assert(writer.ok());

    uint32_t count = list_.count();
    writer << count;
    
    Iterator iter(&list_);
    iter.seek_to_first();

    while (iter.valid()) {
        Msg msg = iter.key();
        uint8_t type = msg.type();

        writer << type << msg.key();
        if (type == Put)
            writer << msg.value();

        count--;
        iter.next();
    }
    assert(count == 0);

    return writer.ok();
}
