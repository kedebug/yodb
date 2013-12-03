#ifndef _YODB_CACHE_H_
#define _YODB_CACHE_H_

#include "db/options.h"
#include "fs/table.h"
#include "fs/file.h"
#include "sys/thread.h"
#include "sys/rwlock.h"
#include "sys/mutex.h"
#include "tree/node.h"

#include <map>

namespace yodb {

class BufferTree;

class Cache {
public:
    Cache(const Options& opts);
    ~Cache();

    bool init();

    // Integrate cache with our buffer tree and Table storage.
    void integrate(BufferTree* tree, Table* table);

    // When we invoke BufferTree::create_node(),
    // the newly created node should put into cache.
    void put(nid_t nid, Node* node);

    // All nodes are managed by our Cache System,
    // if the node is not in the cache, then we will
    // invoke Table::read() to get node buffer from disk.
    Node* get(nid_t nid);

    Timestamp last_checkpoint_timestamp;
private:
    // There is a single thread to write the memory node 
    // to disk, the frequency is defined by Options.
    void write_back();
    void write_complete_handler(Node* node, Slice buffer, Status status);

    void flush_ready_nodes(std::vector<Node*>& nodes);

    void maybe_eviction();
    void evict_from_memory();

private:
    Options options_;
    size_t cache_size_;
    Mutex cache_size_mutex_;

    bool alive_;
    Thread* worker_;

    Table* table_;
    BufferTree* tree_;

    typedef std::map<nid_t, Node*> NodeMap;
    NodeMap nodes_; 
    RWLock lock_nodes_;
};

} // namespace yodb

#endif // _YODB_CACHE_H_
