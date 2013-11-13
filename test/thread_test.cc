#include <gtest/gtest.h>
#include "util/thread.h"

using namespace yodb;

int count1;
static void thr_fn()
{
    count1 += 1;
}

TEST(Thread, run)
{
    Thread thr(thr_fn);
    count1 = 0;
    thr.run();
    thr.join();
    EXPECT_EQ(count1, 1); 
}
