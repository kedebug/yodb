#ifndef _YODB_OPTIONS_H_
#define _YODB_OPTIONS_H_

#include "db/comparator.h"
#include "fs/env.h"

namespace yodb {

class Options {
public:
    Options() 
    {
        comparator = NULL;
        env = NULL;
        max_node_child_number = 16;
        max_node_msg_count    = 64;
    }
    Comparator* comparator;
    Env* env;

    size_t max_node_child_number;
    size_t max_node_msg_count;
    size_t max_node_page_size;
    size_t cache_limited_memory;
    size_t cache_dirty_node_expire;

};

} // namespace yodb

#endif // _YODB_OPTIONS_H_
