#ifndef _YODB_TABLE_H_
#define _YODB_TABLE_H_

#include "fs/file.h"
#include "tree/node.h"
#include "util/block.h"

#include <stdint.h>
#include <map>
#include <deque>

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>

namespace yodb {

#define PAGE_SIZE           4096

#define PAGE_ROUND_UP(x)    (((x) + PAGE_SIZE - 1) & (~(PAGE_SIZE - 1)))
#define PAGE_ROUND_DOWN(x)  ((x) & (~(PAGE_SIZE - 1)))
#define PAGE_ROUNDED(x)     ((x) == (PAGE_ROUND_DOWN(x)))

#define BOOTSTRAP_SIZE      PAGE_SIZE 

struct BlockHandle {
    BlockHandle() : offset(0), size(0) {}

    uint64_t offset;
    uint32_t size;
};

class Bootstrap {
public:
    Bootstrap() : header(), root_nid(NID_NIL) {}

    BlockHandle header;
    nid_t root_nid;
};

// Table for permanent storage
class Table : boost::noncopyable {
public:
    Table(AIOFile* file, uint64_t file_size);
    ~Table();

    bool init(bool create = false);
    
    // Get node's block information marked by nid.
    Block* read(nid_t nid);

    typedef boost::function<void (Status)> Callback;

    // Asynchoronous write file, this will be always called by Cache module.
    void async_write(nid_t nid, Block& block, Callback cb);

    bool flush_bootstrap();
    bool load_bootstrap();

    bool flush_header();
    bool load_header();

    // Flush all the buffers to file.
    bool flush();
    bool flush_immediately();

    void init_holes();

    // Holes are unused parts of our file, we collect it for further usage.
    void add_hole(uint64_t offset, uint32_t size);
    
    // Whether we can get suitable room from hole list.
    // This will be always called by find_space().
    bool get_hole(uint32_t size, uint64_t& offset);

    // Fly holes are collected from asynchoronous write calls:
    // new rooms allocated and old rooms become fly holes. 
    // There will still be some readers since we are in multithreading environment.
    // Fly holes will be able to use if we invoke flush_fly_holes().
    void add_fly_hole(uint64_t offset, uint32_t size);

    void flush_fly_holes(size_t fly_holes);

    nid_t get_root_nid() { return bootstrap_.root_nid; }
    void set_root_nid(nid_t nid) { bootstrap_.root_nid = nid; }

    size_t get_node_count()  
    {
        ScopedMutex lock(block_entry_mutex_);
        return block_entry_.size();
    }

    // Get size of all the block handle, this will be always called by flush_index().
    uint32_t block_header_size();

    // Give a block handle, returns the the block you needed.
    Block* read_block(const BlockHandle* handle);

    // Get suitable room for size, return the offset of our file.
    uint64_t find_space(uint32_t size);

    // Synchoronous read file
    bool read_file(uint64_t offset, Slice& buffer);
    // Synchoronous write file
    bool write_file(uint64_t offset, const Slice& buffer);

    void truncate();

    // We use posix_memalign() to allocate aligned buffer, this is efficiency 
    // since we are using asynchoronous read/write functions.
    Slice self_alloc(size_t size);

    // posix_memalign() allocated buffer should use free() to deallocate.
    void self_dealloc(Slice alloc_ptr);

    // size() function is seldom used, we remain it for debug reason.
    uint64_t size() { return file_size_; }

private:
    AIOFile* file_; 
    uint64_t file_size_;
    uint64_t offset_;
    Mutex mutex_;

    Bootstrap bootstrap_;

    typedef std::map<nid_t, BlockHandle*> BlockEntry;
    BlockEntry block_entry_;
    Mutex block_entry_mutex_;

    struct AsyncWriteContext {
        nid_t nid;
        Callback callback;
        BlockHandle handle;
    };

    void async_write_handler(AsyncWriteContext* context, Status status);

    struct Hole {
        uint64_t offset;
        uint32_t size;
    };

    uint32_t fly_readers_;
    uint32_t fly_writers_;

    typedef std::deque<Hole> HoleList;
    // the holes is sorted by offset
    HoleList hole_list_;
    HoleList fly_hole_list_;

    Mutex hole_list_mutex_;
    Mutex fly_hole_list_mutex_;
};

} // namespace yodb

#endif // _YODB_TABLE_H_
