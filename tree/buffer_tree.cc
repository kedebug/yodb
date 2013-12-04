#include "tree/buffer_tree.h"

using namespace yodb;

BufferTree::BufferTree(const std::string name, Options& opts, 
                       Cache* cache, Table* table)
    : name_(name), options_(opts), 
      cache_(cache), table_(table),
      root_(NULL), node_count_(0), 
      node_map_(), mutex_(), mutex_lock_path_()
{
}

BufferTree::~BufferTree()
{
    // root_ is always referenced
    if (root_)
        root_->dec_ref();

    cache_->flush();

    LOG_INFO << "BufferTree destructor finished";
}

bool BufferTree::init()
{
    cache_->integrate(this, table_);

    nid_t root_nid = table_->get_root_nid();
    node_count_ = table_->get_node_count();

    root_ = get_node_by_nid(root_nid);

    if (root_nid == NID_NIL) {
        assert(root_ == NULL);
        assert(node_count_ == 0);

        root_ = create_node();
        root_->is_leaf_ = true;
        root_->create_first_pivot();
    }

    return root_ != NULL;
}

void BufferTree::grow_up(Node* root)
{
    ScopedMutex lock(mutex_);

    root_->dec_ref();
    root_ = root;
    table_->set_root_nid(root_->self_nid_);
}

Node* BufferTree::create_node()
{
    ScopedMutex lock(mutex_);

    ++node_count_;

    nid_t nid = node_count_;
    Node* node = new Node(this, nid);

    cache_->put(nid, node);
    // node_map_[nid] = node;

    return node;
}

Node* BufferTree::get_node_by_nid(nid_t nid)
{
    ScopedMutex lock(mutex_);
    assert(nid != NID_NIL);
    assert(nid <= node_count_);

    return cache_->get(nid);
    // return node_map_[nid];
}

void BufferTree::lock_path(const Slice& key, std::vector<Node*>& path)
{
    ScopedMutex lock(mutex_lock_path_);

    Node* root = root_;
    root->inc_ref();
    root->write_lock();

    if (root != root_) {
        // Tree maybe grow up after we get the lock, 
        // so we just give up if we miss this action.
        root->write_unlock();
        root->dec_ref();
    } else {
        root->lock_path(key, path);
    }
}

bool BufferTree::put(const Slice& key, const Slice& value)
{
    assert(root_);

    root_->inc_ref();
    bool succ = root_->put(key, value);
    root_->dec_ref();

    return succ;
}

bool BufferTree::del(const Slice& key)
{
    assert(root_);

    root_->inc_ref();
    bool succ = root_->del(key);
    root_->dec_ref();

    return succ;
}

bool BufferTree::get(const Slice& key, Slice& value)
{
    assert(root_);

    root_->inc_ref();
    bool succ = root_->get(key, value);
    root_->dec_ref();

    return succ;
}
