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
        : offset_(0) {}

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
    Block(Slice slice)
        : buffer_(slice), offset_(0), size_(slice.size()) {}

    // a block maybe only a part of the slice
    Block(Slice slice, size_t offset, size_t size)
        : buffer_(slice), offset_(offset), size_(size) 
    {
        assert(size <= slice.size());
        assert(offset + size <= slice.size());
    }
    
    // return data in the range of the block
    const char* data()  { return buffer_.data() + offset_; }
    size_t  size()      { return size_; }
    size_t  avail()     { return size_ - offset_; }
    Slice get_buffer()  { return buffer_; }

private:
    Slice buffer_;
    size_t offset_;
    size_t size_;
};

class BlockReader : boost::noncopyable {
public:
    explicit BlockReader(Block& block)
        : block_(block), offset_(0), succ_(true)
    {
    }

    bool ok() { return succ_; }

    typedef BlockReader self;

    self& operator>>(bool& v) {
        uint8_t val;

        *this >> val;
        if (val) v = true; 
        else v = false;

        return *this;
    }

    self& operator>>(uint8_t& v);
    self& operator>>(uint16_t& v);
    self& operator>>(uint32_t& v);
    self& operator>>(uint64_t& v);
    self& operator>>(Slice& s);

private:
    template<typename T>
    bool read_uint(T& v);

    Block& block_;
    size_t offset_;
    bool succ_;
};

class BlockWriter : boost::noncopyable {
public:
    explicit BlockWriter(Block& block)
        : block_(block), offset_(0), succ_(true)
    {
    }

    bool ok() { return succ_; } 

    typedef BlockWriter self;

    self& operator<<(bool v) {
        uint8_t val;

        if (v) val = 1;
        else val = 0;

        *this << val;
        return *this;
    }

    self& operator<<(uint8_t v);
    self& operator<<(uint16_t v);
    self& operator<<(uint32_t v);
    self& operator<<(uint64_t v);
    self& operator<<(const Slice& s);

private:
    template<typename T>
    bool write_uint(T v);

    Block& block_;
    size_t offset_;
    bool succ_;
};

} // namespace yodb

#endif // _YODB_BLOCK_H_
