#include "util/logger.h"
#include "util/thread.h"
#include <stdlib.h>

namespace yodb {

LogLevel init_logger_level()
{
    char* env = getenv("YODB_LOG_LEVEL");

    if (env == NULL)
        return INFO;
    else if (strcmp(env, "TRACE") == 0)
        return TRACE;
    else if (strcmp(env, "DEBUG") == 0)
        return DEBUG;
    else if (strcmp(env, "INFO") == 0) 
        return INFO;
    else if (strcmp(env, "WARN") == 0)
        return WARN;
    else if (strcmp(env, "ERROR") == 0)
        return ERROR;

    return INFO;
}

void default_output(const char* msg, int len)
{
    fwrite(msg, 1, len, stdout);
}

void default_flush()
{
    fflush(stdout);
}

LogLevel            g_logger_level = init_logger_level();
Logger::output_fn   g_output = default_output;
Logger::flush_fn    g_flush = default_flush; 

const char* LogLevelName[] = {
    "TRACE ", "DEBUG ", "INFO  ", "WARN  ", "ERROR ",
};

} // namespace yodb


using namespace yodb;

Impl::Impl(LogLevel level, const SourceFile& file, int line)
    : stream_(), level_(level), filename_(file), line_(line)    
{
    stream_ << filename_.data() << ':' << line_ << ' ';
    stream_ << LogLevelName[level] << ": ";
    stream_ << Fmt("tid=%d, ", current_thread::get_tid());
}

void Impl::finish()
{
    stream_ << '\n';
}

Logger::Logger(SourceFile file, int line)
    : Impl(INFO, file, line)
{
}

Logger::Logger(SourceFile file, int line, LogLevel level, const char* fn_name)
    : Impl(level, file, line)
{
    stream() << '*' << fn_name << "* ";
}

Logger::Logger(SourceFile file, int line, LogLevel level)
    : Impl(level, file, line)
{
}

LogLevel Logger::logger_level()
{
    return g_logger_level;
}

void Logger::set_flush(flush_fn fn)
{
    g_flush = fn;
}

void Logger::set_output(output_fn fn)
{
    g_output = fn;
}

void Logger::set_logger_level(LogLevel level)
{
    g_logger_level = level;
}

Logger::~Logger()
{
    finish();
    Slice slice = get_logger_data();
    g_output(slice.data(), slice.size());
}
