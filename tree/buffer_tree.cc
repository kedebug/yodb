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
    if (root_) {
        root_->dec_ref();
        assert(root_->refs() == 0);
        table_->set_root_nid(root_->nid());
    }

    LOG_INFO << Fmt("%zu nodes created", node_count_);

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
        root_->set_leaf(true);
        root_->create_first_pivot();
    }

    return root_ != NULL;
}

void BufferTree::grow_up(Node* root)
{
    ScopedMutex lock(mutex_);

    root_->dec_ref();
    root_ = root;
    table_->set_root_nid(root_->nid());
}

Node* BufferTree::create_node()
{
    {
        ScopedMutex lock(mutex_);
        ++node_count_;
    }

    nid_t nid = node_count_;
    Node* node = new Node(this, nid);

    cache_->put(nid, node);

    return node;
}

Node* BufferTree::create_node(nid_t nid)
{
    return new Node(this, nid);
}

Node* BufferTree::get_node_by_nid(nid_t nid)
{
    return cache_->get(nid);
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
    
    // Tree maybe grow up after we insert a kv,
    // so we should use the copy of the root_ to
    // ensure dec_ref() right processed.(same as below)
    Node* root = root_;
    root->inc_ref();
    bool succ = root->put(key, value);
    root->dec_ref();

    return succ;
}

bool BufferTree::del(const Slice& key)
{
    assert(root_);

    Node* root = root_;
    root->inc_ref();
    bool succ = root->del(key);
    root->dec_ref();

    return succ;
}

bool BufferTree::get(const Slice& key, Slice& value)
{
    assert(root_);

    Node* root = root_;
    root->inc_ref();
    bool succ = root->get(key, value);
    root->dec_ref();

    return succ;
}
