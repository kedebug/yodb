#include "util/logger.h"
#include "util/thread.h"

namespace yodb {

LogLevel init_logger_level()
{
    return TRACE;
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
    stream_ << LogLevelName[level] << "| ";
    stream_ << Fmt("tid=%d | ", current_thread::get_tid());
}

void Impl::finish()
{
    stream_ << " | " << filename_.data() << ':' << line_ << '\n';
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
