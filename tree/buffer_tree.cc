#include "tree/buffer_tree.h"

using namespace yodb;

void BufferTree::init_tree()
{
    root_ = create_node(NID_NIL);
}

void BufferTree::grow_up(Node* root)
{
    root_ = root;
}

Node* BufferTree::create_node(nid_t parent)
{
    node_count_ += 1;

    nid_t nid = node_count_;
    Node* node = new Node(this, nid, parent);
    node_map_[nid] = node;

    return node;
}

Node* BufferTree::get_node_by_nid(nid_t nid)
{
    assert(nid <= node_count_);
    return node_map_[nid];
}
