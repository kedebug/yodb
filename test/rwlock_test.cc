#include "sys/rwlock.h"
#include "sys/thread.h"
#include "util/timestamp.h"

#include <pthread.h>
#include <stdio.h>

#include <boost/ptr_container/ptr_vector.hpp>

using namespace yodb;

static const int kCount = 1000 * 10;
static const int kMaxThreads = 4;

RWLock rwlock;
Mutex g_mutex;
pthread_rwlock_t g_lock;

void rwlock_read() 
{
    for (int i = 0; i < kCount; i++) {
        rwlock.read_lock();
        usleep(100);
        rwlock.read_unlock();
    }
}

void rwlock_write() 
{
    for (int i = 0; i < kCount; i++) {
        rwlock.write_lock();
        usleep(100);
        rwlock.write_unlock();
    }
}

void pthread_rwlock_read()
{
    for (int i = 0; i < kCount; i++) {
        pthread_rwlock_rdlock(&g_lock);
        usleep(100);
        pthread_rwlock_unlock(&g_lock);
    }
}

void pthread_rwlock_write()
{
    for (int i = 0; i < kCount; i++) {
        pthread_rwlock_wrlock(&g_lock);
        usleep(100);
        pthread_rwlock_unlock(&g_lock);
    }
}

void mutex_read()
{
    for (int i = 0; i < kCount; i++) {
        ScopedMutex lock(g_mutex);
        usleep(100);
    }
}

void mutex_write()
{
    for (int i = 0; i < kCount; i++) {
        ScopedMutex lock(g_mutex);
        usleep(100);
    }
}

int main()
{
    Timestamp start, finish;
    boost::ptr_vector<Thread> threads;

    // pthread_rwlock_t
    pthread_rwlock_init(&g_lock, NULL); 

    for (int i = 0; i < kMaxThreads / 2; i++) 
        threads.push_back(new Thread(&pthread_rwlock_read));
    for (int i = kMaxThreads / 2; i < kMaxThreads; i++)
        threads.push_back(new Thread(&pthread_rwlock_write));

    start = Timestamp::now();
    for (int i = 0; i < kMaxThreads; i++)
        threads[i].run();
    for (int i = 0; i < kMaxThreads; i++)
        threads[i].join();
    finish = Timestamp::now();

    double time = time_interval(finish, start);
    printf("POSIX: %d threads, %d readers, %d writers, costs = %f\n",
            kMaxThreads, kMaxThreads / 2, kMaxThreads - kMaxThreads / 2, time);
    
    pthread_rwlock_destroy(&g_lock);

    // Mutex
    threads.clear();

    for (int i = 0; i < kMaxThreads / 2; i++) 
        threads.push_back(new Thread(&mutex_read));
    for (int i = kMaxThreads / 2; i < kMaxThreads; i++)
        threads.push_back(new Thread(&mutex_write));

    start = Timestamp::now();
    for (int i = 0; i < kMaxThreads; i++)
        threads[i].run();
    for (int i = 0; i < kMaxThreads; i++)
        threads[i].join();
    finish = Timestamp::now();

    time = time_interval(finish, start);
    printf("Mutex: %d threads, %d readers, %d writers, costs = %f\n",
            kMaxThreads, kMaxThreads / 2, kMaxThreads - kMaxThreads / 2, time);

    // RWLock 
    threads.clear();

    for (int i = 0; i < kMaxThreads / 2; i++) 
        threads.push_back(new Thread(&rwlock_read));
    for (int i = kMaxThreads / 2; i < kMaxThreads; i++)
        threads.push_back(new Thread(&rwlock_write));

    start = Timestamp::now();
    for (int i = 0; i < kMaxThreads; i++)
        threads[i].run();
    for (int i = 0; i < kMaxThreads; i++)
        threads[i].join();
    finish = Timestamp::now();

    time = time_interval(finish, start);
    printf("RWLock: %d threads, %d readers, %d writers, costs = %f\n",
            kMaxThreads, kMaxThreads / 2, kMaxThreads - kMaxThreads / 2, time);
}
