#ifndef _YODB_TIMESTAMP_H_
#define _YODB_TIMESTAMP_H_

#include <stdint.h>
#include <string>

namespace yodb {

class Timestamp {
public:
    Timestamp();
    Timestamp(int64_t microsec_since_epoch);

    std::string to_string();
    static Timestamp now();
    int64_t microseconds() const;

    static const int kMicroPerSecond = 1000 * 1000;
private:
    int64_t microseconds_;
};

inline double time_interval(const Timestamp& x, const Timestamp& y)
{
    int64_t diff_microsec = x.microseconds() - y.microseconds();
    return static_cast<double>(diff_microsec) / Timestamp::kMicroPerSecond;
}

inline bool operator<(const Timestamp& x, const Timestamp& y)
{
    return x.microseconds() < y.microseconds(); 
}

inline bool operator>(const Timestamp& x, const Timestamp& y)
{
    return x.microseconds() > y.microseconds(); 
}

} // namespace yodb

#endif // _YODB_TIMESTAMP_H_
