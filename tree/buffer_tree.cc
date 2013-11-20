#include "tree/buffer_tree.h"

using namespace yodb;

bool BufferTree::init_tree()
{
    root_ = create_node(NID_NIL);
    return root_ != NULL;
}

void BufferTree::grow_up(Node* root)
{
    ScopedMutex lock(mutex_);
    root_ = root;
}

Node* BufferTree::create_node(nid_t parent)
{
    ScopedMutex lock(mutex_);

    ++node_count_;

    nid_t nid = node_count_;
    Node* node = new Node(this, nid, parent);
    node_map_[nid] = node;

    return node;
}

Node* BufferTree::get_node_by_nid(nid_t nid)
{
    ScopedMutex lock(mutex_);
    assert(nid <= node_count_);
    return node_map_[nid];
}

bool BufferTree::put(const Slice& key, const Slice& value)
{
    assert(root_);
    return root_->put(key, value);
}

bool BufferTree::del(const Slice& key)
{
    assert(root_);
    return root_->del(key);
}

bool BufferTree::get(const Slice& key, Slice& value)
{
    assert(root_);
    return root_->get(key, value);
}
