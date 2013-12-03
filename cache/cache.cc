#include "cache/cache.h"
#include "tree/buffer_tree.h"

#include <algorithm>
#include <boost/bind.hpp>

using namespace yodb;

class FirstWriteComparator {
public:
    bool operator()(Node* x, Node* y) 
    {
        return x->get_first_write_timestamp() < y->get_first_write_timestamp();
    }
};

class LRUComparator {
public:
    bool operator()(Node* x, Node* y)
    {
        return x->get_last_used_timestamp() < y->get_last_used_timestamp();
    }
};

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

void Cache::integrate(BufferTree* tree, Table* table)
{
    assert(tree && table);

    tree_ = tree;
    table_ = table;

    last_checkpoint_timestamp = Timestamp::now();
}

void Cache::put(nid_t nid, Node* node)
{
    assert(node->refs() == 0);

    maybe_eviction(); 

    lock_nodes_.write_lock();

    assert(nodes_.find(nid) == nodes_.end());
    nodes_[nid] = node;
    node->inc_ref();

    lock_nodes_.write_unlock();
}

Node* Cache::get(nid_t nid)
{
    lock_nodes_.read_lock();

    NodeMap::iterator iter = nodes_.find(nid);

    if (iter != nodes_.end()) {
        Node* node = iter->second;
        node->inc_ref();
        lock_nodes_.read_unlock();
        return node;
    }

    maybe_eviction();

    Block* block = table_->read(nid);

    if (block == NULL) {
        lock_nodes_.read_unlock();
        return NULL;
    }

    BlockReader reader(*block);

    Node* node = tree_->create_node();
    if (!(node->constrcutor(reader))){
        assert(false);
    }

    table_->self_dealloc(block->buffer());
    nodes_[nid] = node;
    node->inc_ref();

    lock_nodes_.read_unlock();

    return node;
}

void Cache::flush()
{
}

void Cache::write_back()
{
    while (alive_) {
        std::vector<Node*> expired_nodes;
        Timestamp now = Timestamp::now();

        size_t total_size = 0;
        size_t dirty_size = 0;
        size_t expired_size = 0;
        size_t goal = options_.cache_dirty_node_expire / 100;

        lock_nodes_.read_lock();

        for (NodeMap::iterator iter = nodes_.begin(); iter != nodes_.end(); iter++) {
            Node* node = iter->second;
            size_t size = node->size();

            total_size += size;

            if (node->dirty()) {
                dirty_size += size;

                bool expire = (double)options_.cache_dirty_node_expire < 
                        time_interval(now, node->get_first_write_timestamp());

                if (expire && !node->flushing()) {
                    expired_nodes.push_back(node);
                    expired_size += size;
                }
            }
        }
        
        lock_nodes_.read_unlock();
        {
            ScopedMutex lock(cache_size_mutex_);
            cache_size_ = total_size;
        }

        FirstWriteComparator comparator;
        std::sort(expired_nodes.begin(), expired_nodes.end(), comparator);
        
        std::vector<Node*> flush_nodes;
        size_t flush_size = 0;
         
        for (size_t i = 0; i < expired_nodes.size(); i++) {
            Node* node = expired_nodes[i];

            if (node->try_write_lock()) {
                node->set_flushing(true);
                flush_size += node->size();
                flush_nodes.push_back(node);
            }

            if (flush_size > goal) break;
        }

        bool maybe = (dirty_size - flush_size) >
                (options_.cache_limited_memory * 30 / 100);

        if (maybe && flush_size < goal) {
            lock_nodes_.read_lock();

            std::vector<Node*> maybe_nodes;
            for (NodeMap::iterator iter = nodes_.begin();
                    iter != nodes_.end(); iter++) {
                Node* node = iter->second;

                if (node->dirty() && !node->flushing()) 
                    maybe_nodes.push_back(node);
            }

            lock_nodes_.read_unlock();

            for (size_t i = 0; i < maybe_nodes.size(); i++) {
                Node* node = maybe_nodes[i];
                size_t size = node->size();

                if (node->try_write_lock()) {
                    node->set_flushing(true);
                    flush_size += size;
                    flush_nodes.push_back(node);
                }
            }
        }
        
        if (flush_nodes.size())
            flush_ready_nodes(flush_nodes);

        ::usleep(100 * 1000); // 100ms
    }
}

void Cache::flush_ready_nodes(std::vector<Node*>& ready_nodes)
{
    for (size_t i = 0; i < ready_nodes.size(); i++) {
        Node* node = ready_nodes[i];
        size_t size = node->write_back_size();
        
        Slice buffer = table_->self_alloc(size);
        assert(buffer.size());

        Block block(buffer, 0, size);
        BlockWriter writer(block);

        node->destructor(writer);
        assert(writer.ok());

        node->set_dirty(false);
        node->write_unlock();

        table_->async_write(node->self_nid_, block, 0,
            boost::bind(&Cache::write_complete_handler, this, node, buffer, _1)); 
    }

    Timestamp now = Timestamp::now();
    double time = 1000000 * 60;
    if (time_interval(now, last_checkpoint_timestamp) > time) {
        table_->flush_right_now();
        table_->truncate();
        last_checkpoint_timestamp = now;
    }
}

void Cache::write_complete_handler(Node* node, Slice buffer, Status status)
{
    assert(node != NULL);
    assert(buffer.size());

    node->set_flushing(false);
    table_->self_dealloc(buffer);

    if (status.succ) {
        LOG_INFO << "write back node success, nid=" << node->self_nid_;
    } else {
        LOG_ERROR << "write back node failed, nid=" << node->self_nid_;
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
