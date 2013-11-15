#ifndef _YODB_BLOCK_H_
#define _YODB_BLOCK_H_

#include "util/slice.h"
#include <assert.h>
#include <string.h>
#include <boost/noncopyable.hpp>

namespace yodb {

const int kSmallBuffer = 4000;
const int kLargeBuffer = 4000 * 1000;

template<int SIZE>
class FixedBlock {
public:
    FixedBlock()
        : offset_(0)
    {
    }

    size_t avail()
    {
        return SIZE - offset_;
    }

    void append(const Slice& slice) 
    {
        assert(avail() > slice.size());
        memcpy(buffer_ + offset_, slice.data(), slice.size());
        offset_ += slice.size();
    }

    void append(const char* s, size_t len)
    {
        assert(avail() > len);
        memcpy(buffer_ + offset_, s, len);
        offset_ += len;
    }

    Slice get_buffer()
    {
        assert(offset_ < SIZE);
        return Slice(buffer_, offset_);
    }
private:
    char buffer_[SIZE];
    size_t offset_;
};

class Block : boost::noncopyable {
public:
    explicit Block(Slice slice)
        : buffer_(slice), offset_(0)
    {
    }
    
    size_t  capacity()      { return buffer_.size(); }
    size_t  avail()         { return buffer_.size() - offset_; }
    Slice&  get_buffer()    { return buffer_; }

private:
    Slice buffer_;
    size_t offset_;
};

class BlockWriter {
public:
    explicit BlockWriter(Block& block)
        : block_(block)
    {
    }



private:
    Block& block_;
};

}

#endif // _YODB_BLOCK_H_
