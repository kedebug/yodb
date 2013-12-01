#ifndef _YODB_CACHE_H_
#define _YODB_CACHE_H_

#include "db/options.h"
#include "fs/table.h"
#include "sys/thread.h"
#include "tree/node.h"

#include <map>

namespace yodb {

class BufferTree;

class Cache {
public:
    Cache(const Options& opts);
    ~Cache();

    bool init();
    void put(nid_t nid, Node* node);
    Node* get_node_by_nid(nid_t nid);

private:
    void write_back();
    void maybe_eviction();
    void evict_from_memory();

private:
    Options options_;
    uint64_t cache_size_;

    bool alive_;
    Thread* worker_;

    Table* table_;
    BufferTree* tree_;

    typedef std::map<nid_t, Node*> NodeMap;
    NodeMap nodes_; 
};

} // namespace yodb

#endif // _YODB_CACHE_H_
