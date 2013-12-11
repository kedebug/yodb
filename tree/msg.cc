#include "tree/msg.h"

using namespace yodb;

MsgTable::MsgTable(Comparator* comparator)
    : list_(Compare(comparator)), 
      comparator_(comparator), 
      mutex_(), size_(0)
{
}

MsgTable::~MsgTable()
{
    Iterator iter(&list_);
    iter.seek_to_first();

    while (iter.valid()) {
        iter.key().release();
        iter.next();
    }

    list_.clear();
}

size_t MsgTable::count()
{
    return list_.count();
}

size_t MsgTable::size()
{
    return 4 + size_;
}

size_t MsgTable::memory_usage()
{
    return list_.memory_usage() + sizeof(MsgTable);
}

void MsgTable::clear()
{
    assert(mutex_.is_locked_by_this_thread());

    list_.clear();
    size_ = 0;
}

void MsgTable::insert(const Msg& msg)
{
    ScopedMutex lock(mutex_);

    Iterator iter(&list_);
    Msg got;
    bool release = false;
    iter.seek(msg);

    if (iter.valid()) {
        got = iter.key();
        
        if (got.key() == msg.key()) {
            size_ -= got.size();
            assert(got.value() == msg.value()); 
            release = true;
        }
    }

    list_.insert(msg);
    size_ += msg.size();

    if (release)
        got.release();
}

void MsgTable::resize(size_t size)
{
    assert(mutex_.is_locked_by_this_thread());
    list_.resize(size);

    size_ = 0;
    Iterator iter(&list_);
    iter.seek_to_first();

    while (iter.valid()) {
        size_ += iter.key().size(); 
        iter.next();
    }
}

bool MsgTable::find(Slice key, Msg& msg)
{
    assert(mutex_.is_locked_by_this_thread());
    
    Msg fake(_Nop, key);
    Iterator iter(&list_);

    iter.seek(fake);
    
    if (iter.valid() && iter.key().key() == key) {
        msg = iter.key();
        return true;
    }

    return false;
}

bool MsgTable::constrcutor(BlockReader& reader)
{
    assert(reader.ok());

    ScopedMutex lock(mutex_);

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

bool MsgTable::destructor(BlockWriter& writer)
{
    assert(writer.ok());

    ScopedMutex lock(mutex_);

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
