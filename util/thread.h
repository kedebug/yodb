#ifndef _YODB_THREAD_H_
#define _YODB_THREAD_H_

#include "util/mutex.h"

#include <pthread.h>
#include <sys/syscall.h>

#include <string>
#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

namespace yodb {

class Thread : boost::noncopyable {
public:
    typedef boost::function<void()> Function;

    explicit Thread(Function& fn, std::string& name);
    ~Thread();

    void run();
    void join();

private:
    bool running_;
    bool joined_;
    pthread_t tidp_;
    std::string name_;
    boost::shared_ptr<pid_t> tid_;
    Function thread_fn_;
};

namespace current_thread {

    __thread pid_t cached_tid = 0;
    __thread const char* thread_name = "unknown";

    pid_t get_tid()
    {
        if (!cached_tid) {
            cached_tid = static_cast<pid_t>(syscall(SYS_gettid));
        } 
        return cached_tid;
    }
} // namespace current_thread

} // namespace yodb

#endif // _YODB_THREAD_H_
