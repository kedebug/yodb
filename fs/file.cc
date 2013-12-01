#include "fs/file.h"
#include <fcntl.h>
#include <boost/bind.hpp>

using namespace yodb;

AIOFile::AIOFile(const std::string& path)
    : path_(path), fd_(-1), 
      closed_(false), ioctx_(0), thread_(NULL)
{
}

AIOFile::~AIOFile()
{
    close();
}

bool AIOFile::open()
{
    fd_ = ::open(path_.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0644);
    if (fd_ == -1) {
        LOG_ERROR << "open file " << path_ << " error: " << strerror(errno);
        return false;
    }

    int status = io_setup(MAX_AIO_EVENTS, &ioctx_);
    if (status < 0) {
        LOG_ERROR << "io_setup error: " << strerror(status);
        ::close(fd_);
        return false;
    }

    thread_ = new Thread(boost::bind(&AIOFile::handle_io_complete, this));
    assert(thread_);
    thread_->run();

    return true;
}

void AIOFile::close()
{
    if (closed_ == false) {
        thread_->join();
        closed_ = false;
        delete thread_;
        
        int status = io_destroy(ioctx_);
        if (status < 0)
            LOG_ERROR << "io_destroy error: " << strerror(status);

        ::close(fd_);
    }
}

void AIOFile::truncate(uint64_t offset)
{
    if (ftruncate(fd_, offset) < 0)
        LOG_ERROR << "ftruncate error: " << strerror(errno);
}

Status AIOFile::read(uint64_t offset, Slice& buffer)
{
    BIORequest* request = new BIORequest();

    request->mutex.lock();
    async_read(offset, buffer, boost::bind(&BIORequest::complete, request, _1));
    assert(request->mutex.is_locked_by_this_thread());
    request->cond.wait();

    Status stat = request->status;
    request->mutex.unlock();
    
    delete request;
    return stat;
}

Status AIOFile::write(uint64_t offset, const Slice& buffer)
{
    BIORequest* request = new BIORequest();

    request->mutex.lock();
    async_write(offset, buffer, boost::bind(&BIORequest::complete, request, _1));
    assert(request->mutex.is_locked_by_this_thread());
    request->cond.wait();

    Status stat = request->status;
    request->mutex.unlock();
    
    delete request;
    return stat;
}

void AIOFile::async_read(uint64_t offset, Slice& buffer, Callback cb)
{
    struct iocb iocb;
    struct iocb* iocbs = &iocb;

    AIORequest* request = new AIOReadRequest();
    request->size = buffer.size();
    request->callback = cb;

    io_prep_pread(&iocb, fd_, (void*)buffer.data(), buffer.size(), offset);
    iocb.data = request;

    int status;
    do {
        status = io_submit(ioctx_, 1, &iocbs);

        if (-status == EAGAIN) {
            usleep(1000);
        } else if (status < 0){
            LOG_ERROR << "io_submit error: " << strerror(-status);
            request->complete(status);
            delete request;
            break;
        }
    } while (status < 0);
}

void AIOFile::async_write(uint64_t offset, const Slice& buffer, Callback cb)
{
    struct iocb iocb;
    struct iocb* iocbs = &iocb;

    AIORequest* request = new AIOWriteRequest();
    request->size = buffer.size();
    request->callback = cb;

    io_prep_pwrite(&iocb, fd_, (void*)buffer.data(), buffer.size(), offset);
    iocb.data = request;

    int status;
    do {
        status = io_submit(ioctx_, 1, &iocbs);

        if (-status == EAGAIN) {
            usleep(1000);
        } else if (status < 0){
            LOG_ERROR << "io_submit error: " << strerror(-status);
            request->complete(status);
            delete request;
            break;
        }
    } while (status < 0);
}

void AIOFile::handle_io_complete()
{
    while (!closed_) {
        struct io_event events[MAX_AIO_EVENTS];
        memset(events, 0, sizeof(io_event) * MAX_AIO_EVENTS);
        
        struct timespec timeout;
        timeout.tv_sec  = 0;
        timeout.tv_nsec = 100000000;

        int num_events = io_getevents(ioctx_, 1, MAX_AIO_EVENTS, events, &timeout);
        if (num_events < 0) {
            LOG_ERROR << "io_getevents error: " << strerror(-num_events);
            if (-num_events != EINTR) 
                break;
            else 
                continue;
        }

        for (int i = 0; i < num_events; i++) {
            AIORequest* req = static_cast<AIORequest*>(events[i].data);
            req->complete(events[i].res);
            delete req;
        }
    } 
}
