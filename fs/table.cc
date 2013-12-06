#include "fs/table.h"
#include <stdlib.h>
#include <boost/bind.hpp>

using namespace yodb;

Table::Table(AIOFile* file, uint64_t file_size)
    : file_(file), file_size_(file_size), offset_(0),
      fly_readers_(0), fly_writers_(0)
{
}

Table::~Table()
{
    // if (!flush()) {
    //     LOG_ERROR << "flush error";
    //     assert(false);
    // }

    BlockIndex::iterator iter;
    ScopedMutex lock(block_index_mutex_);

    for (iter = block_index_.begin(); iter != block_index_.end(); iter++) {
        BlockMeta* meta = iter->second;
        delete meta;
    }

    block_index_.clear();

    LOG_INFO << "Table destructor finished";
}

bool Table::flush()
{
    mutex_.lock();
    while (fly_writers_) {
        mutex_.unlock();
        usleep(1000);
        mutex_.lock();
    }
    mutex_.unlock();

    if (!flush_right_now())
        return false;

    truncate();
    return true;
}

bool Table::flush_right_now()
{
    size_t fly_holes;
    {
        ScopedMutex lock(fly_hole_list_mutex_);
        fly_holes = fly_hole_list_.size();
    }

    if (!flush_index()) {
        LOG_ERROR << "flush_index error";
        return false;
    }

    if (!flush_superblock()) {
        LOG_ERROR << "flush_superblock error";
        return false;
    }

    // for (size_t i = 0; i < hole_list_.size(); i++) {
    //     LOG_INFO << Fmt("hole: offset=%zu, ", hole_list_[i].offset)
    //              << Fmt("size=%zu", hole_list_[i].size);
    // }
    flush_fly_holes(fly_holes);

    return true;
}

void Table::flush_fly_holes(size_t fly_holes)
{
    ScopedMutex lock(fly_hole_list_mutex_);

    while (fly_holes) {
        Hole hole = fly_hole_list_.front();
        add_hole(hole.offset, hole.size);
        fly_hole_list_.pop_front();
        fly_holes--;
    }
}

bool Table::init(bool create)
{
    if (create) {
        if (!flush_superblock()) {
            LOG_ERROR << "flush_superblock error";
            return false;
        }
        offset_ = SUPER_BLOCK_SIZE;
        file_size_ = SUPER_BLOCK_SIZE;
    } else {
        if (file_size_ < SUPER_BLOCK_SIZE) {
            LOG_ERROR << "must be load the wrong file";
            return false;
        }

        if (!load_superblock()) {
            LOG_ERROR << "load superblock error";
            return false;
        }
        LOG_INFO << "load_superblock success";

        if (superblock_.index_meta.offset) {
            if (!load_index()) {
                LOG_ERROR << "load index error";
                return false;
            }
            LOG_INFO << "load_index success";
        }

        init_holes();
        LOG_INFO << block_index_.size() << " blocks found";
    }   
    truncate();
    return true;
}

void Table::init_holes()
{
    ScopedMutex lock(block_index_mutex_);

    std::map<uint64_t, BlockMeta*> offset_set; 
    offset_set[superblock_.index_meta.offset] = &superblock_.index_meta;

    BlockIndex::iterator iter;
    for (iter = block_index_.begin(); iter != block_index_.end(); iter++) {
        offset_set[iter->second->offset] = iter->second;
    }

    std::map<uint64_t, BlockMeta*>::iterator curr, prev;
    for (curr = offset_set.begin(); curr != offset_set.end(); curr++) {
        uint64_t left;

        if (curr == offset_set.begin())
            left = SUPER_BLOCK_SIZE;
        else 
            left = prev->second->offset + PAGE_ROUND_UP(prev->second->total_size);

        if (left < curr->second->offset)
            add_hole(left, curr->second->offset - left);

        prev = curr;
    }

    if (offset_set.size() > 1)
        offset_ = offset_set.rbegin()->second->offset +
            PAGE_ROUND_UP(offset_set.rbegin()->second->total_size);
    else
        offset_ = SUPER_BLOCK_SIZE;
}

void Table::add_hole(uint64_t offset, uint32_t size)
{
    {
        ScopedMutex lock(mutex_);
        if (offset + size == offset_) {
            offset_ = offset;
            return ;
        }
    }

    Hole hole;
    hole.size = size;
    hole.offset = offset;

    ScopedMutex lock(hole_list_mutex_);

    if (hole_list_.empty()) {
        hole_list_.push_back(hole);
        return;
    }

    HoleList::iterator iter, prev;
    for (iter = hole_list_.begin(); iter != hole_list_.end(); iter++) {
        if (iter->offset > hole.offset)
            break;
        prev = iter;
    }

    if (iter != hole_list_.end()) {
        assert(hole.offset + hole.size <= iter->offset);
        if (iter != hole_list_.begin()) {
            // LOG_INFO << Fmt("offset=%zu, ", (iter-1)->offset)
            //          << Fmt("size=%zu, ", (iter-1)->size)
            //          << Fmt("new hole offset=%zu", hole.offset);
            assert(prev->offset + prev->size <= hole.offset);

            if (prev->offset + prev->size == hole.offset) {
                hole.offset = prev->offset;
                hole.size += prev->size;
                hole_list_.erase(prev);
            }
        }

        if (hole.offset + hole.size == iter->offset) {
            iter->offset = hole.offset;
            iter->size += hole.size;
        } else {
            hole_list_.insert(iter, hole);
        }
    } else {
        if (prev->offset + prev->size == hole.offset)
            prev->size += hole.size;
        else 
            hole_list_.push_back(hole);
    }
}

bool Table::get_hole(uint32_t size, uint64_t& offset)
{
    ScopedMutex lock(hole_list_mutex_);
    HoleList::iterator iter;

    for (iter = hole_list_.begin(); iter != hole_list_.end(); iter++) {
        if (iter->size > size) {
            iter->size -= size;
            offset = iter->offset;
            iter->offset += size;
            return true;
        } else {
            offset = iter->offset;
            hole_list_.erase(iter);
            return true;
        }
    }
    return false;
}

void Table::add_fly_hole(uint64_t offset, uint32_t size)
{
    assert(PAGE_ROUNDED(size));

    Hole hole;
    hole.offset = offset;
    hole.size = size;

    ScopedMutex lock(fly_hole_list_mutex_);
    fly_hole_list_.push_back(hole);
}

void Table::truncate()
{
    ScopedMutex lock(mutex_);

    if (offset_ <= file_size_) {
        file_->truncate(offset_);
        file_size_ = offset_;
        LOG_INFO << Fmt("truncate, file size=%zuK", file_size_/1024);
    }
}

bool Table::flush_superblock()
{
    Slice slice = self_alloc(SUPER_BLOCK_SIZE);
    if (!slice.size()) {
        LOG_ERROR << "self_alloc failed.";
        return false;
    }

    Block block(slice, 0, SUPER_BLOCK_SIZE); 
    BlockWriter writer(block);
    
    bool maybe = superblock_.index_meta.offset == 0 ? false : true;
    writer << maybe;
    if (writer.ok() && maybe) {
        write_block_meta(superblock_.index_meta, writer);
        writer << superblock_.root_nid;
    }

    if (!writer.ok()) {
        LOG_ERROR << "write_block_meta error";
        return false;
    }

    if (!write_file(0, slice)) {
        LOG_ERROR << "write_file error";
        return false;
    }

    LOG_INFO << "flush_superblock success";
    self_dealloc(slice);
    return true;
}

bool Table::load_superblock()
{
    Slice buffer = self_alloc(SUPER_BLOCK_SIZE);
    if (!buffer.size()) {
        LOG_ERROR << "self_alloc error";
        return false;
    }
    if (!read_file(0, buffer)) {
        LOG_ERROR << "read superblock failed";
        self_dealloc(buffer);
        return false;
    }

    Block block(buffer, 0, SUPER_BLOCK_SIZE);
    BlockReader reader(block);
    bool maybe = false;

    reader >> maybe;
    if (reader.ok() && maybe) {
        reader >> superblock_.index_meta.offset 
               >> superblock_.index_meta.index_size 
               >> superblock_.index_meta.total_size
               >> superblock_.root_nid; 
    }

    if (!reader.ok()) 
        LOG_ERROR << "read superblock failed";

    self_dealloc(buffer);
    return reader.ok();
}

bool Table::flush_index()
{
    uint32_t index_size = get_index_size();
    Slice buffer = self_alloc(index_size);

    if (buffer.size() == 0) {
        LOG_ERROR << "self_alloc failed";
        return false;
    }

    Block block(buffer, 0, index_size);
    BlockWriter writer(block);
   
    {
        ScopedMutex lock(block_index_mutex_);

        uint32_t num_index = block_index_.size();
        BlockIndex::iterator iter;

        writer << num_index;
        for (iter = block_index_.begin(); iter != block_index_.end(); iter++) {
            nid_t nid = iter->first;
            BlockMeta* meta = iter->second;
            
            writer << nid;
            write_block_meta(*meta, writer);
        }

        if (!writer.ok()) {
            LOG_ERROR << "flush_index error";
            self_dealloc(buffer);
            return false;
        }
    }

    uint64_t offset = get_room(buffer.size());
    Status stat = file_->write(offset, buffer);

    if (!stat.succ) {
        LOG_ERROR << "write file error";
        add_hole(offset, buffer.size());
        self_dealloc(buffer);
        return false;
    }

    LOG_INFO << "flush_index success, " 
             << Fmt("offset=%zu, ", offset)
             << Fmt("size=%zu", index_size);

    if (superblock_.index_meta.offset) {
        add_fly_hole(superblock_.index_meta.offset, 
                     PAGE_ROUND_UP(superblock_.index_meta.total_size));
    }

    superblock_.index_meta.offset = offset;
    superblock_.index_meta.total_size = index_size;

    self_dealloc(buffer);
    return true;
}

bool Table::load_index()
{
    assert(superblock_.index_meta.offset > 0); 

    Block* block = read_block(superblock_.index_meta, 0, 
                            superblock_.index_meta.total_size);
    if (block == NULL) {
        LOG_ERROR << "read_block failed";
        return false;
    }

    BlockReader reader(*block);
    uint32_t num_index = 0;
    ScopedMutex lock(block_index_mutex_);

    reader >> num_index;
    while (reader.ok() && num_index > 0) {
        nid_t nid;
        BlockMeta* meta = new BlockMeta();
        assert(meta);

        reader >> nid;
        read_block_meta(*meta, reader);

        if (reader.ok()) {
            block_index_[nid] = meta;
        } else {
            delete meta;
            LOG_ERROR << "read_block_meta error";
        }
        num_index--;
    }

    if (!reader.ok())
        LOG_ERROR << "load_index error";

    self_dealloc(block->buffer());
    delete block;
    return reader.ok();
}

uint32_t Table::get_index_size()
{
    ScopedMutex lock(block_index_mutex_);

    uint32_t size_num_index = 4;
    uint32_t num_index = block_index_.size();
    uint32_t size_block_meta = sizeof(nid_t) + sizeof(BlockMeta);

    return size_num_index + num_index * size_block_meta;
}

Block* Table::read(nid_t nid)
{
    BlockMeta* meta;

    {
        ScopedMutex lock(block_index_mutex_);

        BlockIndex::iterator iter = block_index_.find(nid); 
        if (iter == block_index_.end()) return NULL;

        meta = iter->second;
        assert(meta);
    }

    if (nid == 50)
        LOG_INFO << "nid=50, read offset=" << meta->offset;
    Block* block = read_block(*meta, 0, meta->total_size); 

    //LOG_INFO << Fmt("read node success, nid=%zu", nid);
    return block;
}

void Table::async_write(nid_t nid, Block& block, uint32_t index_size, Callback cb)
{
    assert(block.buffer().size() == PAGE_ROUND_UP(block.size())); 
    
    AsyncWriteContext* context = new AsyncWriteContext();
    context->nid = nid;
    context->callback = cb;
    context->meta.index_size = index_size;
    context->meta.total_size = block.size();
    context->meta.offset = get_room(block.buffer().size());
    {
        ScopedMutex lock(mutex_);
        fly_writers_++;
    }

    // LOG_INFO << Fmt("get room, nid=%zu, ", nid)
    //          << Fmt("offset=%zu, ", context->meta.offset)
    //          << Fmt("size=%zu", block.buffer().size());

    if (nid == 50) {
        BlockReader reader(block);
        nid_t self, parent;
        reader >> self >> parent;
        LOG_INFO << Fmt("self=%zu, ", self) << Fmt("parent=%zu, ", parent)
                 << Fmt("write offset=%zu, ", context->meta.offset);
    }
    file_->async_write(context->meta.offset, block.buffer(), 
                boost::bind(&Table::async_write_handler, this, context, _1));
}

void Table::async_write_handler(AsyncWriteContext* context, Status status)
{
    {
        ScopedMutex lock(mutex_);
        fly_writers_--;
    }

    if (status.succ) {
        ScopedMutex lock(block_index_mutex_);

        BlockIndex::iterator iter = block_index_.find(context->nid); 
        if (iter == block_index_.end()) {
            BlockMeta* meta = new BlockMeta();
            *meta = context->meta;
            block_index_[context->nid] = meta;
        } else {
            BlockMeta* meta = iter->second;
            add_fly_hole(meta->offset, PAGE_ROUND_UP(meta->total_size));
            *meta = context->meta;
        }
    } else {
        LOG_ERROR << "async_write error, " << Fmt("nid=%zu", context->nid);
        add_hole(context->meta.offset, context->meta.total_size);
    }

    context->callback(status);
    delete context;
}

uint64_t Table::get_room(uint32_t size) 
{
    uint64_t offset;

    if (get_hole(size, offset)) 
        return offset;

    ScopedMutex lock(mutex_);
    // no hole suit for us, append the file
    offset = offset_;
    offset_ += size;

    if (offset_ > file_size_)
        file_size_ = offset_;

    return offset;
}

Block* Table::read_block(BlockMeta& meta, uint32_t offset, uint32_t size)
{
    // uint32_t align_offset = PAGE_ROUND_DOWN(offset);
    // uint32_t fixed_size = size + (offset - align_offset);

    // Slice buffer = self_alloc(fixed_size);
    // if (buffer.size() == 0) {
    //     LOG_ERROR << "self_alloc error";
    //     return NULL;
    // }

    // if (!read_file(meta.offset + align_offset, buffer)) {
    //     LOG_ERROR << "read_file failed";
    //     self_dealloc(buffer);
    //     return NULL;
    // }

    assert(offset == 0);
    Slice buffer = self_alloc(size);
    assert(buffer.size());

    if (!read_file(meta.offset, buffer)) {
        LOG_ERROR << "read_file failed";
        self_dealloc(buffer);
        return NULL;
    }

    return new Block(buffer, 0, size);
}

bool Table::read_block_meta(BlockMeta& meta, BlockReader& reader)
{
    reader >> meta.offset >> meta.index_size >> meta.total_size;
    return reader.ok();
}

bool Table::write_block_meta(BlockMeta& meta, BlockWriter& writer)
{
    writer << meta.offset << meta.index_size << meta.total_size;
    return writer.ok();
}

bool Table::read_file(uint64_t offset, Slice& buffer)
{
    { 
        ScopedMutex lock(mutex_); 
        fly_readers_++; 
    }

    Status status = file_->read(offset, buffer);

    {
        ScopedMutex lock(mutex_);
        fly_readers_--;
    }

    if (!status.succ) {
        LOG_ERROR << "read file error, " << Fmt("offset=%zu.", offset);
        return false;
    }
    return true;
}

bool Table::write_file(uint64_t offset, const Slice& buffer) 
{
    {
        ScopedMutex lock(mutex_);
        fly_writers_++;
    }

    Status status = file_->write(offset, buffer);

    {
        ScopedMutex lock(mutex_);
        fly_writers_--;
    }

    if (!status.succ) {
        LOG_ERROR << "write file error, " << Fmt("offset=%zu.", offset);
        return false;
    }
    return true;
}

Slice Table::self_alloc(size_t size)
{
    size_t align_size = PAGE_ROUND_UP(size);
    void* buffer = NULL;

    if (posix_memalign(&buffer, PAGE_SIZE, align_size) != 0) {
        LOG_ERROR << "posix_memalign error: " << strerror(errno);
        return Slice();
    }
    assert(buffer);
    return Slice((char*)buffer, align_size);
}

void Table::self_dealloc(Slice buffer)
{
    if (buffer.size())
        free((char*)buffer.data());
}
