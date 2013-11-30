#include "tree/buffer_tree.h"

using namespace yodb;

bool BufferTree::init_tree()
{
    root_ = create_node();
    root_->is_leaf_ = true;
    root_->create_first_pivot();
    return root_ != NULL;
}

void BufferTree::grow_up(Node* root)
{
    ScopedMutex lock(mutex_);
    root_ = root;
}

Node* BufferTree::create_node()
{
    ScopedMutex lock(mutex_);

    ++node_count_;

    nid_t nid = node_count_;
    Node* node = new Node(this, nid);
    node_map_[nid] = node;

    return node;
}

Node* BufferTree::get_node_by_nid(nid_t nid)
{
    ScopedMutex lock(mutex_);
    assert(nid != NID_NIL);
    assert(nid <= node_count_);
    return node_map_[nid];
}

void BufferTree::lock_path(const Slice& key, std::vector<nid_t>& path)
{
    ScopedMutex lock(mutex_lock_path_);
    Node* root = root_;
    root->write_lock();
    if (root != root_) 
        root->write_unlock();
    else 
        root->lock_path(key, path);
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
