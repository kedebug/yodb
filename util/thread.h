#ifndef _YODB_THREAD_H_
#define _YODB_THREAD_H_

#include <pthread.h>
#include <sys/syscall.h>

#include <string>
#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

using namespace std;

namespace yodb {

class Thread : boost::noncopyable {
public:
    typedef boost::function<void()> Function;

    explicit Thread(const Function&, const string& name = string());
    ~Thread();

    void run();
    void join();

    pid_t get_tid()
    {
        return static_cast<pid_t>(*tid_);
    }

    bool is_main_thread() 
    {
        return get_tid() == getpid();
    }
private:
    bool alive_;
    bool joined_;
    pthread_t tidp_;
    string name_;
    boost::shared_ptr<pid_t> tid_;
    Function thread_fn_;
};

namespace current_thread {

extern __thread const char* thread_name;
extern __thread pid_t cached_tid;
extern pid_t get_tid();

} // namespace current_thread

} // namespace yodb

#endif // _YODB_THREAD_H_
