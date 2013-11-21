#include "util/log_stream.h"
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

using namespace yodb;

template<typename T>
void LogStream::format_integer(T value)
{
    if (log_buffer_.avail() < 32) 
        return;

    char buf[64];
    char* p = buf;
    T v = value > 0 ? value : -value;

    while (v) {
        int lsd = static_cast<int>(v % 10);
        v /= 10;
        *p++ = lsd + '0';
    }

    if (value < 0) *p++ = '-';
    else if (value == 0) *p++ = '0';
    *p = '\0';
    std::reverse(buf, p);

    log_buffer_.append(buf, p - buf);
}

LogStream& LogStream::operator<< (short v)
{
    *this << static_cast<int>(v);
    return *this;
}

LogStream& LogStream::operator<< (unsigned short v)
{
    *this << static_cast<unsigned int>(v);
    return *this;
}

LogStream& LogStream::operator<< (int v)
{
    format_integer(v);
    return *this;
}

LogStream& LogStream::operator<< (unsigned int v)
{
    format_integer(v);
    return *this;
}

LogStream& LogStream::operator<< (long v)
{
    format_integer(v);
    return *this;
}

LogStream& LogStream::operator<< (unsigned long v)
{
    format_integer(v);
    return *this;
}

LogStream& LogStream::operator<< (long long v)
{
    format_integer(v);
    return *this;
}

LogStream& LogStream::operator<< (unsigned long long v)
{
    format_integer(v);
    return *this;
}

template<typename T>
Fmt::Fmt(const char* fmt, T val)
{
    length_ = snprintf(buf_, sizeof(buf_), fmt, val);
    assert(static_cast<size_t>(length_) < sizeof(buf_));
}

template Fmt::Fmt(const char* fmt, char);

template Fmt::Fmt(const char* fmt, short);
template Fmt::Fmt(const char* fmt, unsigned short);
template Fmt::Fmt(const char* fmt, int);
template Fmt::Fmt(const char* fmt, unsigned int);
template Fmt::Fmt(const char* fmt, long);
template Fmt::Fmt(const char* fmt, unsigned long);
template Fmt::Fmt(const char* fmt, long long);
template Fmt::Fmt(const char* fmt, unsigned long long);

template Fmt::Fmt(const char* fmt, float);
template Fmt::Fmt(const char* fmt, double);

