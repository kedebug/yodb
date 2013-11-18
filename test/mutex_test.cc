#include "util/thread.h"
#include "util/mutex.h"
#include "util/timestamp.h"
#include <boost/ptr_container/ptr_container.hpp>
#include <gtest/gtest.h>

using namespace yodb;

const int kCount = 10 * 1000 * 1000;

Mutex g_mutex;
std::vector<int> g_container;

void thr_fn()
{
    for (int i = 0; i < kCount; i++) {
        ScopedMutex lock(g_mutex);
        g_container.push_back(i);
    }
}

int main()
{
    {
        // basic test for ScopedMutex
        ScopedMutex lock(g_mutex);
        assert(g_mutex.is_locked_by_this_thread());
    } 

    Timestamp start, finish;
    int thread_count = 8;

    // g_container.reserve(kCount * thread_count);

    start = Timestamp::now();
    for (int i = 0; i < kCount; i++) {
        g_container.push_back(i);    
    }
    printf("single thread without lock %f\n", 
            time_interval(Timestamp::now(), start)); 

    start = Timestamp::now();
    g_container.clear();
    thr_fn();
    printf("single thread with lock %f\n", 
            time_interval(Timestamp::now(), start)); 

    for (int i = 1; i < thread_count; i++) {
        boost::ptr_vector<Thread> threads;
        g_container.clear();      

        start = Timestamp::now();
        for (int j = 0; j < i; j++) {
            threads.push_back(new Thread(&thr_fn));
            threads.back().run(); 
        }
        for (int j = 0; j < i; j++) {
            threads[j].join();
        }
        finish = Timestamp::now();
        
        double time = time_interval(finish, start);
        printf("%d thread(s) with lock %f, average=%f\n", 
                i, time, time / (double)i);
    }
}
