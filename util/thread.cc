#include "util/thread.h"
#include <boost/weak_ptr.hpp>

using namespace yodb;

struct ThreadData {
    typedef boost::function<void()> Function;

    Function fn_;
    std::string name_;
    boost::weak_ptr<pid_t> weak_tid_;

    ThreadData(Function& fn, std::string& name, boost::shared_ptr<int> tid)
        : fn_(fn), name_(name), weak_tid_(tid)
    {
    }

    void run_in_data()
    {
        boost::shared_ptr<pid_t> shared_tid = weak_tid_.lock();
        
        if (shared_tid) {
            *shared_tid = current_thread::get_tid();
            shared_tid.reset();
        }

        current_thread::thread_name = name_.c_str();
        fn_();
        current_thread::thread_name = "thread finished";
    }
};

void* thread_start_fn(void* arg)
{
    ThreadData* data = (ThreadData*)arg;
    data->run_in_data();
    delete data;

    return NULL;
}

Thread::Thread(Function& fn, std::string& name)
    : running_(false), joined_(false), tidp_(0), 
      name_(name), tid_(new pid_t), thread_fn_(fn)
{
}

Thread::~Thread()
{
    if (running_ && !joined_) 
        pthread_detach(tidp_);
}

void Thread::run()
{
    ThreadData* data = new ThreadData(thread_fn_, name_, tid_);
    
    running_ = true;
    if (pthread_create(&tidp_, NULL, &thread_start_fn, (void*)data)) {
        running_ = false;
        delete data;
    }
}

void Thread::join()
{
    assert(running_);
    assert(!joined_);

    joined_ = true;
    pthread_join(tidp_, NULL);
}
