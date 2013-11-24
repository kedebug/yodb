#ifndef _YODB_MSG_H_
#define _YODB_MSG_H_

#include "util/slice.h"
#include "util/logger.h"
#include "sys/mutex.h"
#include "db/comparator.h"

#include <vector>

namespace yodb {

enum MsgType {
    _Nop, Put, Del,
};

class Msg {
public:
    Msg() {}
    Msg(MsgType type, Slice key, Slice value = Slice())
        : type_(type), key_(key), value_(value) 
    {
    }

    size_t size()
    {
        return sizeof(MsgType) + sizeof(Slice) * 2 + key_.size() + value_.size(); 
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

class MsgBuf {
public:
    typedef std::vector<Msg> Container;
    typedef Container::iterator Iterator;

    MsgBuf(Comparator* comparator)
        : msgbuf_(), 
          comparator_(comparator),
          mutex_()
    {
    }

    size_t msg_count();

    // release the ownership of the Msg, but not delete them.
    void release();

    // you must lock hold the lock before use it
    Iterator find(Slice key);

    void insert(const Msg& msg);
    void insert(Iterator pos, Iterator first, Iterator last);

    // resize the msgbuf, release but not delete the truncated Msg
    void resize(size_t size);

    Iterator begin()    { return msgbuf_.begin(); }
    Iterator end()      { return msgbuf_.end(); }
    void lock()         { mutex_.lock(); }
    void unlock()       { mutex_.unlock(); }

private:
public:
    Container msgbuf_;
    Comparator* comparator_;
    Mutex mutex_;
};

} // namespace yodb

#endif // _YODB_MSG_H_
