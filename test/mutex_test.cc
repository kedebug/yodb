#include "util/thread.h"
#include "util/mutex.h"
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <gtest/gtest.h>

using namespace yodb;

Mutex mtx;
static void thr_fn()
{
    ScopedMutex scoped_lock(mtx);
    usleep(100000);
}

TEST(Mutex, lock)
{
    Thread thr1(thr_fn);
    Thread thr2(thr_fn);

    struct timeval now, end;
    gettimeofday(&now, NULL);

    thr1.run();
    thr2.run();
    thr1.join();
    thr2.join();

    gettimeofday(&end, NULL);

    long cost = (end.tv_sec - now.tv_sec) * 1000000 + end.tv_usec - now.tv_usec;
    
    EXPECT_NEAR(200000, cost, 5000);
}
