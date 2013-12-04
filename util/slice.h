#ifndef _YODB_SLICE_H_
#define _YODB_SLICE_H_

#include <string.h>
#include <assert.h>
#include <string>

namespace yodb {

class Slice {
public:
    Slice() 
        : data_(""), size_(0), is_self_alloc_(false) {}
    Slice(const char* s) 
        : data_(s), size_(strlen(s)), is_self_alloc_(false) {}
    Slice(const char* s, size_t size, bool is_self_alloc = false) 
        : data_(s), size_(size), is_self_alloc_(is_self_alloc) {}
    Slice(const std::string& s) 
        : data_(s.data()), size_(s.size()), is_self_alloc_(false) {}

    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    void clear() 
    { 
        data_ = ""; 
        size_ = 0; 
        is_self_alloc_ = false;
    }
    
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
        if (size_ == 0)
            return Slice("", 0, true);

        char* s = new char[size_];
        assert(s);
        memcpy(s, data_, size_);
        return Slice(s, size_, true);
    }

    static Slice alloc(size_t size)
    {
        assert(size);
        char* s = new char[size];
        return Slice(s, size, true);
    }

    void release() 
    {
        assert(is_self_alloc_);

        if (size_ == 0) return;

        delete[] data_;
        clear();
    }

private:
    const char* data_;
    size_t size_;
    bool is_self_alloc_;
};

inline bool operator== (const Slice& x, const Slice& y)
{
    return x.compare(y) == 0;
}

inline bool operator!= (const Slice& x, const Slice& y)
{
    return !(x == y);
}

inline bool operator< (const Slice& x, const Slice& y)
{
    return x.compare(y) < 0;
}

} // namespace yodb

#endif // _YODB_SLICE_H_
