#include "cache/cache.h"
#include "tree/buffer_tree.h"

#include <algorithm>
#include <boost/bind.hpp>

using namespace yodb;

Cache::Cache(const Options& opts)
    : options_(opts), cache_size_(0),
      alive_(false), worker_(NULL)
{
}

Cache::~Cache()
{
    alive_ = false;
    if (worker_) {
        worker_->join();
        delete worker_;
        LOG_INFO << "Cache write back work thread finished.";
    }

    LOG_INFO << "Cache destructor finished";
}

bool Cache::init()
{
    alive_ = true;
    worker_ = new Thread(boost::bind(&Cache::write_back, this));

    if (worker_ == NULL) {
        LOG_ERROR << "create thread error";
        return false;
    }

    worker_->run();
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

    lock_nodes_.read_unlock();

    maybe_eviction();

    Block* block = table_->read(nid);
    if (block == NULL) return NULL;

    BlockReader reader(*block);

    Node* node = tree_->create_node(nid);
    if (!(node->constrcutor(reader))){
        assert(false);
    }

    table_->self_dealloc(block->buffer());

    lock_nodes_.write_lock();

    assert(nodes_.find(nid) == nodes_.end());
    nodes_[nid] = node;
    node->inc_ref();

    lock_nodes_.write_unlock();

    return node;
}

void Cache::flush()
{
    lock_nodes_.write_lock();

    std::vector<Node*> ready_nodes;

    for (NodeMap::iterator it = nodes_.begin(); it != nodes_.end(); it++) {
        Node* node = it->second;

        if (node->dirty() && !node->flushing()) {
            node->write_lock();
            node->set_flushing(true);
            ready_nodes.push_back(node);
        }
    }

    lock_nodes_.write_unlock();

    if (ready_nodes.size())
        flush_ready_nodes(ready_nodes);

    table_->flush();
}

void Cache::write_back()
{
    while (alive_) {
        std::vector<Node*> expired_nodes;
        Timestamp now = Timestamp::now();

        size_t total_size = 0;
        size_t dirty_size = 0;
        size_t expired_size = 0;
        size_t goal = options_.cache_limited_memory / 100;

        lock_nodes_.read_lock();

        for (NodeMap::iterator iter = nodes_.begin(); iter != nodes_.end(); iter++) {
            Node* node = iter->second;
            size_t size = node->size();

            total_size += size;

            if (node->dirty()) {
                dirty_size += size;

                bool expire = 20.0 * options_.cache_dirty_node_expire < 
                        time_interval(now, node->get_first_write_timestamp());

                if (expire && !node->flushing()) {
                    expired_nodes.push_back(node);
                    expired_size += size;
                }
            }
        }

        LOG_INFO << Fmt("Memory total size: %zuK, ", total_size / 1024)
                 << Fmt("expire size: %zuK, ", expired_size / 1024)
                 << Fmt("cache size: %zuK", options_.cache_limited_memory / 1024);

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

        LOG_INFO << Fmt("Memory dirty size: %zuK", dirty_size / 1024);

        bool maybe = (dirty_size - flush_size) >
                (options_.cache_limited_memory / 100 * 30);

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
        
        LOG_INFO << Fmt("Memory flush size: %zuK", flush_size / 1024);

        if (flush_nodes.size())
            flush_ready_nodes(flush_nodes);

        ::usleep(1000 * 1000); // 1s
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

    if (!status.succ) {
        LOG_ERROR << "write back node failed, nid=" << node->self_nid_;
    }
}

void Cache::maybe_eviction()
{
    while (true) {
        {
            ScopedMutex lock(cache_size_mutex_);
            if (cache_size_ < options_.cache_limited_memory)
                break;
        }
        evict_from_memory();
        ::usleep(1000);
    }
}

void Cache::evict_from_memory()
{
    // Apply write lock, don't allow any get/put operation,
    // it is guaranteed no increase reference during this period.
    lock_nodes_.write_lock(); 

    size_t total_size = 0;
    std::vector<Node*> candidates;

    for (NodeMap::iterator it = nodes_.begin(); it != nodes_.end(); it++) {
        Node* node = it->second;
        assert(node->self_nid_ == it->first);

        size_t size = node->size();
        total_size += size;

        if (node->refs() == 0 && !node->dirty() && !node->flushing())
            candidates.push_back(node);
    }

    {
        ScopedMutex lock(cache_size_mutex_);
        cache_size_ = total_size;
    }

    size_t evict_size = 0;
    size_t goal = options_.cache_limited_memory / 100;
    std::vector<Node*> evict_nodes;
    LRUComparator comparator;


    std::sort(candidates.begin(), candidates.end(), comparator);

    for (size_t i = 0; i < candidates.size(); i++) {
        Node* node = candidates[i];
        size_t size = node->size();

        assert(node->refs() == 0);
        assert(!node->dirty());
        assert(!node->flushing());

        evict_size += size;
        nodes_.erase(node->self_nid_);

        delete node;

        if (evict_size >= goal) break;
    }

    {
        ScopedMutex lock(cache_size_mutex_);
        cache_size_ -= evict_size;
    }

    lock_nodes_.write_unlock();

    LOG_INFO << Fmt("evict %zuK bytes from memory", evict_size / 1024);
}
