#ifndef _YODB_FILE_H_
#define _YODB_FILE_H_

#include "sys/thread.h"
#include "util/slice.h"
#include "util/logger.h"

#include <libaio.h>
#include <boost/noncopyable.hpp>
#include <boost/function.hpp>

namespace yodb {

#define MAX_AIO_EVENTS  100

struct AIOStatus {
    AIOStatus() : succ(false), size(0) {}
    bool succ;
    size_t size;
};

class AIORequest {
public:
    typedef boost::function<void (AIOStatus)> Callback;

    size_t size;
    Callback callback;

    virtual void complete(int result) = 0;

    virtual ~AIORequest() {}
};

class AIOReadRequest : public AIORequest, boost::noncopyable {
public:
    void complete(int result)
    {
        AIOStatus status;

        if (result < 0) {
            LOG_ERROR << "AIOFile::async_read error: " << result;
            status.succ = false;
        } else {
            status.succ = true;
            status.size = result;
        }

        callback(status); 
    }
};

class AIOWriteRequest : public AIORequest, boost::noncopyable {
public:
    void complete(int result)
    {
        AIOStatus status;

        if (result < 0) {
            LOG_ERROR << "AIOFile::async_write error: " << result;
            status.succ = false;    
        } else if (result < static_cast<int>(size)) {
            LOG_ERROR << "AIOFile::async_write incomplete, expected="
                      << size << ", actually=" << result;
            status.succ = false;
            status.size = result;
        } else {
            status.succ = true;
            status.size = result;
        }

        callback(status);
    }
};

class AIOFile : boost::noncopyable {
public:
    AIOFile(const std::string& path);
    ~AIOFile();

    bool open();
    void close();

    void read(uint64_t offset, Slice& buffer);
    void write(uint64_t offset, const Slice& buffer);
    
    typedef AIORequest::Callback Callback;

    void async_read(uint64_t offset, Slice& buffer, Callback cb);
    void async_write(uint64_t offset, const Slice& buffer, Callback cb);

private:
    void handle_io_complete();

    std::string path_;
    int fd_;
    bool closed_;
    io_context_t ioctx_;
    Thread* thread_;
};

} // namespace yodb

#endif // _YODB_FILE_H_
