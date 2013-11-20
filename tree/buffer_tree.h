#ifndef _YODB_BUFFER_TREE_H_
#define _YODB_BUFFER_TREE_H_

#include "db/options.h"
#include "util/slice.h"
#include "tree/node.h"
#include "util/mutex.h"

#include <map>
#include <string>

namespace yodb {

class Node;

class BufferTree {
public:
    BufferTree(const std::string name, Options& opts)
        : name_(name), 
          options_(opts), 
          root_(NULL),
          node_count_(0),
          node_map_(),
          mutex_()
    {
    }

    bool init_tree();
    void grow_up(Node* root);
    
    bool put(const Slice& key, const Slice& value);
    bool del(const Slice& key);
    bool get(const Slice& key, Slice& value);

    Node* create_node(nid_t parent);
    Node* get_node_by_nid(nid_t nid);

private:
public:
    friend class Node;

    std::string name_;
    Options options_;
    Node* root_; 
    nid_t node_count_;
    std::map<nid_t, Node*> node_map_;
    Mutex mutex_;
};

} // namespace yodb

#endif // _YODB_BUFFER_TREE_H_
