#ifndef _YODB_BLOCK_H_
#define _YODB_BLOCK_H_

#include "util/slice.h"
#include <assert.h>
#include <string.h>
#include <boost/noncopyable.hpp>

namespace yodb {

class Block : boost::noncopyable {
public:
    explicit Block(Slice slice)
        : buffer_(slice), offset_(0)
    {
    }
    
    size_t  capacity()      { return buffer_.size(); }
    size_t  avail()         { return buffer_.size() - offset_; }
    Slice&  get_buffer()    { return buffer_; }

    void append(const char* src, size_t length)
    {
        assert(length < avail());
        char* dst = const_cast<char*>(buffer_.data());
        strncpy(dst + offset_, src, length);
        offset_ += length;
    }

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
