#ifndef _YODB_LOGSTREAM_H_
#define _YODB_LOGSTREAM_H_

#include "util/block.h"
#include "util/slice.h"
#include <boost/noncopyable.hpp>

namespace yodb {

class LogStream : boost::noncopyable {
public:
    LogStream()
        : log_buffer_()
    {
    }

    Slice get_stream_data()
    {
        return log_buffer_.buffer();
    }

    typedef LogStream self;

    self& operator<< (bool v)
    {
        log_buffer_.append(v ? "1" : "0", 1);
        return *this;
    }

    self& operator<< (short);
    self& operator<< (unsigned short);
    self& operator<< (int);
    self& operator<< (unsigned int);
    self& operator<< (long);
    self& operator<< (unsigned long);
    self& operator<< (long long);
    self& operator<< (unsigned long long);


    self& operator<< (const char* s)
    {
        log_buffer_.append(s, strlen(s));
        return *this;
    }

    self& operator<< (char c)
    {
        log_buffer_.append(&c, 1);
        return *this;
    }

    self& operator<< (const std::string& s)
    {
        log_buffer_.append(s.c_str(), s.size());
        return *this;
    }

    inline void append(const char* s, size_t length) 
    {
        log_buffer_.append(Slice(s, length));
    }
private:
    template<typename T> void format_integer(T);
    FixedBlock<kSmallBuffer> log_buffer_;
};

class Fmt {
public:
    template<typename T> Fmt(const char*, T);

    const char* data() const  { return buf_; }
    size_t length() const     { return length_; }
private:
    char buf_[32];
    size_t length_;
};

inline LogStream& operator<< (LogStream& s, const Fmt& fmt)
{
    s.append(fmt.data(), fmt.length()); 
    return s;
}

} // namespace yodb

#endif // _YODB_LOGSTREAM_H_

