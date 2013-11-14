#ifndef _YODB_LOGGER_H_
#define _YODB_LOGGER_H_

#include "util/log_stream.h"
#include <stdio.h>
#include <string.h>

namespace yodb {

enum LogLevel {
    TRACE, DEBUG, INFO, WARN, ERROR,
};

class SourceFile {
public:
    template<int N>
    inline SourceFile(const char (&array)[N])
        : data_(array), size_(N-1)
    {
        const char* slash = strrchr(data_, '/');
        if (slash) {
            data_ = slash + 1;
            size_ -= static_cast<int>(data_ - array);
        }
    }

    explicit SourceFile(const char* filename)
        : data_(filename)
    {
        const char* slash = strrchr(filename, '/');
        if (slash) data_ = slash + 1;
        size_ = static_cast<int>(strlen(data_));
    }

    const char* data() { return data_; }
private:
    const char* data_;
    int size_;
};

class Impl {
public:
    explicit Impl(LogLevel level, const SourceFile& file, int line);
    void finish(); 
    LogStream& stream() { return stream_; }

private:
    LogStream stream_;
    LogLevel level_;
    SourceFile filename_;
    int line_;
};

class Logger : public Impl {
public:
    Logger(SourceFile file, int line);
    Logger(SourceFile file, int line, LogLevel level);
    Logger(SourceFile file, int line, LogLevel level, const char* fn);
    ~Logger();

    Slice get_logger_data()
    {
        return stream().get_stream_data();
    }

    static LogLevel logger_level();
    static void set_logger_level(LogLevel level);

    typedef void (* output_fn)(const char* msg, int len);
    typedef void (* flush_fn)();

    static void set_output(output_fn);
    static void set_flush(flush_fn);
};

#define LOG_TRACE if (yodb::Logger::logger_level() <= yodb::TRACE) \
  yodb::Logger(__FILE__, __LINE__, yodb::TRACE, __func__).stream()
#define LOG_DEBUG if (yodb::Logger::logger_level() <= yodb::DEBUG) \
  yodb::Logger(__FILE__, __LINE__, yodb::DEBUG, __func__).stream()
#define LOG_INFO if (yodb::Logger::logger_level() <= yodb::INFO) \
  yodb::Logger(__FILE__, __LINE__).stream()
#define LOG_WARN yodb::Logger(__FILE__, __LINE__, yodb::WARN).stream()
#define LOG_ERROR yodb::Logger(__FILE__, __LINE__, yodb::ERROR).stream()

} // namespace yodb

#endif // _YODB_LOGGER_H_
