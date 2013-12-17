#ifndef _YODB_DB_H_
#define _YODB_DB_H_

#include "db/comparator.h"
#include "db/options.h"
#include "fs/env.h"
#include "util/slice.h"

namespace yodb {

class DB {
public:
    static DB* open(const std::string& dbname, const Options& opts);

    virtual bool put(Slice key, Slice value) = 0;
    virtual bool get(Slice key, Slice& value) = 0;
    virtual bool del(Slice key) = 0;
};

} // namespace yodb

#endif // _YODB_DB_H_
