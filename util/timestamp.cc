#include "util/timestamp.h"
#include <stdio.h>
#include <string.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#undef __STDC_FORMAT_MACROS
#include <sys/time.h>

using namespace yodb;

Timestamp::Timestamp() 
    : microseconds_(0)
{
}

Timestamp::Timestamp(int64_t microsec_since_epoch)
    : microseconds_(microsec_since_epoch)
{
}

Timestamp Timestamp::now()
{
    struct timeval tv;
    int64_t seconds;

    gettimeofday(&tv, NULL);
    seconds = tv.tv_sec;

    return Timestamp(seconds * kMicroPerSecond + tv.tv_usec);
}

int64_t Timestamp::microseconds() const
{
    return microseconds_;
}

std::string Timestamp::to_string()
{
    char time[32];
    memset(time, 0, sizeof(time));
    
    int64_t sec = microseconds_ / kMicroPerSecond;
    int64_t microsec = microseconds_ % kMicroPerSecond;

    snprintf(time, sizeof(time), "%" PRId64 ".%06" PRId64 "", sec, microsec);

    return time;
}
