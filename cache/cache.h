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

    // When we invoke BufferTree::create_node(),
    // the newly created node should put into cache.
    void put(nid_t nid, Node* node);

    // All nodes are managed by our Cache System,
    // if the node is not in the cache, then we will
    // invoke Table::read() to get node buffer from disk.
    Node* get_node_by_nid(nid_t nid);

private:
    // There is a single thread to write the memory node 
    // to disk, the frequency is defined by Options.
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
