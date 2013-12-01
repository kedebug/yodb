#include "cache/cache.h"
#include "tree/buffer_tree.h"
#include <boost/bind.hpp>

using namespace yodb;

Cache::Cache(const Options& opts)
    : options_(opts), cache_size_(0),
      alive_(false), worker_(NULL)
{
}

Cache::~Cache()
{
}

bool Cache::init()
{
    alive_ = true;
    worker_ = new Thread(boost::bind(&Cache::write_back, this));

    if (worker_ == NULL) {
        LOG_ERROR << "create thread error";
        return false;
    }

    return true;
}

void Cache::put(nid_t nid, Node* node)
{
    maybe_eviction(); 

    assert(nodes_.find(nid) == nodes_.end());
    nodes_[nid] = node;
}

Node* Cache::get_node_by_nid(nid_t nid)
{
    NodeMap::iterator iter = nodes_.find(nid);

    if (iter != nodes_.end()) {
        return iter->second;
    }

    maybe_eviction();

    Block* block = table_->read(nid);
    BlockReader reader(*block);

    if (block == NULL) return NULL;

    Node* node = tree_->create_node();
    if (!(node->constrcutor(reader))){
        assert(false);
    }

    table_->self_dealloc(block->buffer());

    nodes_[nid] = node;
    return node;
}

void Cache::write_back()
{
    while (alive_) {

    }
}

void Cache::maybe_eviction()
{
    while (cache_size_ >= options_.cache_limited_memory) {
        evict_from_memory();
        usleep(1000);
    }
}

void Cache::evict_from_memory()
{
}
