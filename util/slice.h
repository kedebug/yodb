#ifndef _YODB_SLICE_H_
#define _YODB_SLICE_H_

#include <string.h>
#include <assert.h>
#include <string>

namespace yodb {

class Slice {
public:
    Slice() : data_(""), size_(0) {}
    Slice(const char* s) : data_(s), size_(strlen(s)) {}
    Slice(const char* s, size_t size) : data_(s), size_(size) {}
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }
    void clear() { data_ = ""; size_ = 0; }
    
    char operator[] (size_t i) const 
    {
        assert(i < size());
        return data_[i];
    }

    int compare(const Slice& slice) const
    {
        int prefix_len = (size_ < slice.size()) ? size_ : slice.size();
        int res = memcmp(data_, slice.data(), prefix_len);
        if (res == 0) {
            if (size_ < slice.size()) res = -1;
            else if (size_ > slice.size()) res = 1;
        }
        return res;
    }

    Slice clone() const 
    {
        assert(size_);
        char* s = new char[size_];
        assert(s);
        memcpy(s, data_, size_);
        return Slice(s, size_);
    }

    void release() 
    {
        assert(size_);
        delete[] data_;
        clear();
    }

private:
    const char* data_;
    size_t size_;
};

inline bool operator== (const Slice& x, const Slice& y)
{
    return x.compare(y);
}

inline bool operator!= (const Slice& x, const Slice& y)
{
    return !(x == y);
}

} // namespace yodb

#endif // _YODB_SLICE_H_
