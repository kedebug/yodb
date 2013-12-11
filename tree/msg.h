#ifndef _YODB_MSG_H_
#define _YODB_MSG_H_

#include "db/comparator.h"
#include "util/slice.h"
#include "util/logger.h"
#include "sys/mutex.h"
#include "tree/skiplist.h"

#include <vector>

namespace yodb {

enum MsgType {
    _Nop, Put, Del,
};

class Msg {
public:
    Msg() : type_(_Nop) {}
    Msg(MsgType type, Slice key, Slice value = Slice())
        : type_(type), key_(key), value_(value) {}

    size_t size() const
    {
        size_t size = 0;

        size += 1;                      // MsgType->uint8_t
        size += 4 + key_.size();        // Slice->(see BlockWriter<<(Slice))
        if (type_ == Put)
            size += 4 + value_.size();  // Same as key_

        return size;
    }

    void release()
    {
        key_.release();
        if (value_.size()) 
            value_.release();
    }

    Slice key()    const { return key_; }
    Slice value()  const { return value_; }
    MsgType type() const { return type_; }

private:
    MsgType type_;
    Slice key_;
    Slice value_;
};

class Compare {
public:
    Compare(Comparator* comparator)
        : comparator_(comparator) {}

    int operator()(const Msg& a, const Msg& b) const
    {
        return comparator_->compare(a.key(), b.key());
    }
private:
    Comparator* comparator_;
};

class MsgTable {
public:
    typedef SkipList<Msg, Compare> List;
    typedef List::Iterator Iterator;

    MsgTable(Comparator* comparator);
    ~MsgTable();

    size_t count();

    size_t size();

    size_t memory_usage();

    // Clear the Msg, but not delete the memory they allocated.
    void clear();

    // you must lock hold the lock before use it
    bool find(Slice key, Msg& msg);

    void insert(const Msg& msg);
    void insert(Iterator pos, Iterator first, Iterator last);

    bool constrcutor(BlockReader& reader);
    bool destructor(BlockWriter& writer);

    // resize the msgbuf, release but not delete the truncated Msg
    void resize(size_t size);

    void lock()         { mutex_.lock(); }
    void unlock()       { mutex_.unlock(); }

private:
public:
    List list_;
    Comparator* comparator_;
    Mutex mutex_;
    size_t size_;
};

} // namespace yodb

#endif // _YODB_MSG_H_
