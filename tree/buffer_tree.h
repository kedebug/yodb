#ifndef _YODB_BUFFER_TREE_H_
#define _YODB_BUFFER_TREE_H_

#include "db/options.h"
#include "fs/table.h"
#include "cache/cache.h"
#include "util/slice.h"
#include "tree/node.h"
#include "sys/mutex.h"

#include <map>
#include <string>

namespace yodb {

class Node;

class BufferTree {
public:
    BufferTree(const std::string name, Options& opts, Cache* cache, Table* table);
    ~BufferTree();

    bool init();
    void grow_up(Node* root);
    
    bool put(const Slice& key, const Slice& value);
    bool del(const Slice& key);
    bool get(const Slice& key, Slice& value);

    // Create a newly node without known the nid.
    Node* create_node();

    // This kind of create_node with nid is usually called by cache,
    // typically because nid have already exists, so no need to produce a new one.
    Node* create_node(nid_t nid);

    Node* get_node_by_nid(nid_t nid);
    void  lock_path(const Slice& key, std::vector<Node*>& path);

private:
    friend class Node;

    std::string name_;
    Options options_;
    Cache* cache_;
    Table* table_;
    Node* root_; 
    nid_t node_count_;
    std::map<nid_t, Node*> node_map_;
    Mutex mutex_;
    Mutex mutex_lock_path_;
};

} // namespace yodb

#endif // _YODB_BUFFER_TREE_H_
