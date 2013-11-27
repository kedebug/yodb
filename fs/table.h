#ifndef _YODB_TABLE_H_
#define _YODB_TABLE_H_

#include "fs/file.h"
#include "tree/node.h"
#include "util/block.h"

#include <stdint.h>
#include <map>
#include <deque>
#include <boost/noncopyable.hpp>

namespace yodb {

#define PAGE_SIZE           4096

#define PAGE_ROUND_UP(x)    (((x) + PAGE_SIZE - 1) & (~(PAGE_SIZE - 1)))
#define PAGE_ROUND_DOWN(x)  ((x) & (~(PAGE_SIZE - 1)))
#define PAGE_ROUNDED(x)     ((x) == (PAGE_ROUND_DOWN(x)))

#define SUPER_BLOCK_SIZE    PAGE_SIZE 

struct BlockMeta {
    BlockMeta()
        : offset(0), index_size(0), total_size(0) {}

    uint64_t offset;
    uint32_t index_size;
    uint32_t total_size;
};

class SuperBlock {
public:
    SuperBlock() 
        : index_meta() {}

    BlockMeta index_meta;
};

// Table for permanent storage
class Table : boost::noncopyable {
public:
    Table(AIOFile* file, uint64_t file_size)
        : file_(file), 
          file_size_(file_size),
          offset_(0)
    {
    }

    bool init(bool create = false);
    void init_holes();
    void truncate();
    void add_hole(uint64_t offset, uint32_t size);
    bool get_hole(uint32_t size, uint64_t& offset);

    bool flush_superblock();
    bool load_superblock();

    bool load_index();

    uint64_t get_room(uint32_t size);

    Block* read_block(BlockMeta& meta, uint32_t offset, uint32_t size);
    bool read_block_meta(BlockMeta& meta, BlockReader& reader);
    bool write_block_meta(BlockMeta& meta, BlockWriter& writer);

    bool read_file(uint64_t offset, Slice& buffer);
    bool write_file(uint64_t offset, const Slice& buffer);

private:
    AIOFile* file_; 
    uint64_t file_size_;
    uint64_t offset_;
    SuperBlock superblock_;

    typedef std::map<nid_t, BlockMeta*> BlockIndex;
    BlockIndex block_index_;


    Slice self_alloc(size_t size);
    void self_dealloc(Slice buffer);

    struct Hole {
        uint64_t offset;
        uint32_t size;
    };

    typedef std::deque<Hole> HoleList;
    // the holes is sorted by offset
    HoleList hole_list_;
};

} // namespace yodb

#endif // _YODB_TABLE_H_
