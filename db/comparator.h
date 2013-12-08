#ifndef _YODB_COMPARATOR_H_
#define _YODB_COMPARATOR_H_

#include "util/slice.h"

namespace yodb {

class Comparator {
public:
    virtual int compare(const Slice& a, const Slice& b) const = 0;
};

class BytewiseComparator : public Comparator {
public:
    int compare(const Slice& a, const Slice& b) const { return a.compare(b); }
};

} // namespace yodb

#endif // _YODB_COMPARATOR_H_
