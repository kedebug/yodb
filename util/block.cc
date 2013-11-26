#include "util/block.h"

using namespace yodb;

BlockReader& BlockReader::operator>>(uint8_t& v)
{
    if (!succ_) return *this;

    succ_ = read_uint(v);
    return *this;
}

BlockReader& BlockReader::operator>>(uint16_t& v)
{
    if (!succ_) return *this;

    succ_ = read_uint(v);
    return *this;
}

BlockReader& BlockReader::operator>>(uint32_t& v)
{
    if (!succ_) return *this;

    succ_ = read_uint(v);
    return *this;
}

BlockReader& BlockReader::operator>>(uint64_t& v)
{
    if (!succ_) return *this;

    succ_ = read_uint(v);
    return *this;
}

BlockReader& BlockReader::operator>>(Slice& s)
{
    if (!succ_) return *this;

    uint32_t size;
    succ_ = read_uint(size);

    if (succ_) {
        assert(offset_ <= block_.size());
        if (offset_ + size <= block_.size()) {
            s = Slice(block_.data() + offset_, size).clone();
            offset_ += size;
        } else {
            succ_ = false;
        }
    }
    return *this;
}

template<typename T>
bool BlockReader::read_uint(T& v)
{
    assert(offset_ <= block_.size()); 

    if (offset_ + sizeof(T) <= block_.size()) {
        v = *(T*)(block_.data() + offset_);
        offset_ += sizeof(T);
        return true;
    } else {
        return false;
    }
}

BlockWriter& BlockWriter::operator<<(uint8_t v)
{
    if (!succ_) return *this;

    succ_ = write_uint(v);
    return *this;
}

BlockWriter& BlockWriter::operator<<(uint16_t v)
{
    if (!succ_) return *this;

    succ_ = write_uint(v);
    return *this;
}

BlockWriter& BlockWriter::operator<<(uint32_t v)
{
    if (!succ_) return *this;

    succ_ = write_uint(v);
    return *this;
}

BlockWriter& BlockWriter::operator<<(uint64_t v)
{
    if (!succ_) return *this;

    succ_ = write_uint(v);
    return *this;
}

BlockWriter& BlockWriter::operator<<(const Slice& s)
{
    if (!succ_) return *this;

    uint32_t size = s.size();
    succ_ = write_uint(size);

    if (succ_) {
        assert(offset_ <= block_.size());
        if (offset_ + size <= block_.size()) {
            memcpy((char*)block_.data() + offset_, s.data(), size);
            offset_ += size;
        } else {
            succ_ = false;
        }
    }
    return *this;
}

template<typename T>
bool BlockWriter::write_uint(T v)
{
    assert(offset_ <= block_.size());
    if (offset_ + sizeof(T) <= block_.size()) {
        *(T*)(block_.data() + offset_) = v;
        offset_ += sizeof(T);
        return true;
    } else {
        return false;
    }
}
