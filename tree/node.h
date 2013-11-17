#ifndef _YODB_NODE_H_
#define _YODB_NODE_H_

#include "util/slice.h"
#include "tree/msg.h"

#include <stdint.h>
#include <vector>

namespace yodb {

typedef uint64_t nid_t; 

#define NID_NIL     ((nid_t)0)

class BufferTree;

class Pivot {
public:
    Pivot() {}
    Pivot(nid_t child, MsgBuf* mbuf, Slice key = Slice())
        : msgbuf(mbuf), child_nid(child), left_most_key_(key) {}

    Slice left_most_key()
    {
        // when left_most_key() function is called, 
        // the left_most_key must be exists(Actually, bad design).
        assert(left_most_key_.size());
        return left_most_key_;
    }

    MsgBuf* msgbuf;
    nid_t child_nid;
private:
    Slice left_most_key_;
};

class Node {
public:
    Node(BufferTree* tree, nid_t self, nid_t parent)
        : tree_(tree), 
          self_nid_(self), 
          parent_nid_(parent), 
          node_page_size_(0), 
          pivots_()
    {
    }

    bool get(Slice key, Slice& value);

    bool put(Slice key, Slice value)
    {
        return write(Msg(Put, key.clone(), value.clone()));
    }

    bool del(Slice key)
    {
        return write(Msg(Del, key.clone()));
    }

    bool write(const Msg& msg);

    // when the leaf node's number of pivot is out of limit,
    // it then will split the node and push up the split operation.
    void try_split_node();

    // find which pivot matches the key
    size_t find_pivot(Slice key);
    void add_pivot(Slice key, nid_t child);

    // internal node would push down the msgbuf when it is full 
    void push_down_msgbuf(size_t pivot_index);

    // only the leaf node would split msgbuf when it is full
    void split_msgbuf(size_t pivot_index);

    typedef std::vector<Pivot> Container;

private:
public:
    BufferTree* tree_;
    nid_t self_nid_;
    nid_t parent_nid_;

    size_t node_page_size_;
    Container pivots_; 
};

} // namespace yodb

#endif // _YODB_NODE_H_
