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
    if (!flush()) {
        LOG_ERROR << "flush error";
        assert(false);
    }

    BlockEntry::iterator iter;
    ScopedMutex lock(block_entry_mutex_);

    for (iter = block_entry_.begin(); iter != block_entry_.end(); iter++) {
        BlockHandle* handle = iter->second;
        delete handle;
    }

    block_entry_.clear();

    LOG_INFO << "Table destructor finished";
}

bool Table::init(bool create)
{
    if (create) {
        if (!flush_bootstrap()) return false;

        offset_ = BOOTSTRAP_SIZE;
        file_size_ = BOOTSTRAP_SIZE;
    } else {
        if (file_size_ < BOOTSTRAP_SIZE) return false;
        if (!load_bootstrap()) return false;

        if (bootstrap_.header.offset) {
            if (!load_header()) return false;
        }

        init_holes();
        LOG_INFO << block_entry_.size() << " blocks found";
    }   
    truncate();
    return true;
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

    if (!flush_immediately())
        return false;

    truncate();
    return true;
}

bool Table::flush_immediately()
{
    size_t fly_holes;
    {
        ScopedMutex lock(fly_hole_list_mutex_);
        fly_holes = fly_hole_list_.size();
    }

    if (!flush_header()) return false;
    if (!flush_bootstrap()) return false;

    flush_fly_holes(fly_holes);

    return true;
}

bool Table::flush_bootstrap()
{
    Slice alloc_ptr = self_alloc(BOOTSTRAP_SIZE);
    assert(alloc_ptr.size());

    Block block(alloc_ptr, 0, BOOTSTRAP_SIZE); 
    BlockWriter writer(block);
    
    bool maybe = bootstrap_.header.offset == 0 ? false : true;
    writer << maybe;
    if (writer.ok() && maybe) {
        writer << bootstrap_.header.offset 
               << bootstrap_.header.size
               << bootstrap_.root_nid;
    }

    assert(writer.ok());

    if (!write_file(0, alloc_ptr)) {
        LOG_INFO << "flush_bootstrap error";
        return false;
    }

    LOG_INFO << "flush_bootstrap success, "
             << Fmt("offset=%zu, ", bootstrap_.header.offset)
             << Fmt("size=%zu, ", bootstrap_.header.size)
             << Fmt("root nid=%zu", bootstrap_.root_nid);

    self_dealloc(alloc_ptr);
    return true;
}

bool Table::load_bootstrap()
{
    Slice alloc_ptr = self_alloc(BOOTSTRAP_SIZE);
    assert(alloc_ptr.size());

    if (!read_file(0, alloc_ptr)) {
        LOG_ERROR << "load bootstrap failed";
        self_dealloc(alloc_ptr);
        return false;
    }

    Block block(alloc_ptr, 0, BOOTSTRAP_SIZE);
    BlockReader reader(block);
    bool maybe = false;

    reader >> maybe;
    if (reader.ok() && maybe) {
        reader >> bootstrap_.header.offset 
               >> bootstrap_.header.size
               >> bootstrap_.root_nid; 
    }

    if (!reader.ok()) 
        LOG_ERROR << "read bootstrap failed";
    else 
        LOG_INFO << "load_bootstrap success, "
                 << Fmt("offset=%zu, ", bootstrap_.header.offset)
                 << Fmt("size=%zu, ", bootstrap_.header.size)
                 << Fmt("root nid=%zu", bootstrap_.root_nid);

    self_dealloc(alloc_ptr);
    return reader.ok();
}

bool Table::flush_header()
{
    uint32_t header_size = block_header_size();
    Slice alloc_ptr = self_alloc(header_size);

    assert(alloc_ptr.size());

    Block block(alloc_ptr, 0, header_size);
    BlockWriter writer(block);
   
    {
        ScopedMutex lock(block_entry_mutex_);

        uint32_t blocks = block_entry_.size();
        BlockEntry::iterator iter;

        writer << blocks;
        for (iter = block_entry_.begin(); iter != block_entry_.end(); iter++) {
            nid_t nid = iter->first;
            BlockHandle* handle = iter->second;
            
            writer << nid << handle->offset << handle->size;
        }

        if (!writer.ok()) {
            LOG_ERROR << "flush_header error";
            self_dealloc(alloc_ptr);
            return false;
        }
    }

    uint64_t offset = find_space(alloc_ptr.size());
    Status stat = file_->write(offset, alloc_ptr);

    if (!stat.succ) {
        LOG_ERROR << "write file error";
        add_hole(offset, alloc_ptr.size());
        self_dealloc(alloc_ptr);
        return false;
    }

    LOG_INFO << "flush_header success, " 
             << Fmt("offset=%zu, ", offset)
             << Fmt("size=%zu", header_size);

    if (bootstrap_.header.offset) {
        add_fly_hole(bootstrap_.header.offset, 
                     PAGE_ROUND_UP(bootstrap_.header.size));
    }

    bootstrap_.header.offset = offset;
    bootstrap_.header.size = header_size;

    self_dealloc(alloc_ptr);

    return true;
}

bool Table::load_header()
{
    assert(bootstrap_.header.offset > 0); 

    Block* block = read_block(&bootstrap_.header);
    if (block == NULL) {
        LOG_ERROR << "read_block failed";
        return false;
    }

    BlockReader reader(*block);
    uint32_t blocks = 0;
    ScopedMutex lock(block_entry_mutex_);

    reader >> blocks;
    while (reader.ok() && blocks > 0) {
        nid_t nid;
        BlockHandle* handle = new BlockHandle();
        assert(handle);

        reader >> nid >> handle->offset >> handle->size;

        if (reader.ok())
            block_entry_[nid] = handle;
        else
            delete handle;
        
        blocks--;
    }

    if (!reader.ok())
        LOG_ERROR << "load_header error";
    else 
        LOG_INFO << "load_header success";

    self_dealloc(block->buffer());
    delete block;
    return reader.ok();
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

void Table::init_holes()
{
    ScopedMutex lock(block_entry_mutex_);

    std::map<uint64_t, BlockHandle*> offset_set; 
    offset_set[bootstrap_.header.offset] = &bootstrap_.header;

    BlockEntry::iterator iter;
    for (iter = block_entry_.begin(); iter != block_entry_.end(); iter++) {
        offset_set[iter->second->offset] = iter->second;
    }

    std::map<uint64_t, BlockHandle*>::iterator curr, prev;
    for (curr = offset_set.begin(); curr != offset_set.end(); curr++) {
        uint64_t left;

        if (curr == offset_set.begin())
            left = BOOTSTRAP_SIZE;
        else 
            left = prev->second->offset + PAGE_ROUND_UP(prev->second->size);

        if (left < curr->second->offset)
            add_hole(left, curr->second->offset - left);

        prev = curr;
    }

    if (offset_set.size() > 1)
        offset_ = offset_set.rbegin()->second->offset +
            PAGE_ROUND_UP(offset_set.rbegin()->second->size);
    else
        offset_ = BOOTSTRAP_SIZE;
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
        } else if (iter->size == size) {
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


uint32_t Table::block_header_size()
{
    ScopedMutex lock(block_entry_mutex_);

    uint32_t size_blocks = 4;
    uint32_t blocks = block_entry_.size();
    uint32_t size_block_handle = sizeof(nid_t) + sizeof(BlockHandle);

    return size_blocks + blocks * size_block_handle;
}

Block* Table::read(nid_t nid)
{
    BlockHandle* handle;

    {
        ScopedMutex lock(block_entry_mutex_);

        BlockEntry::iterator iter = block_entry_.find(nid); 
        if (iter == block_entry_.end()) return NULL;

        handle = iter->second;
        assert(handle);
    }

    Block* block = read_block(handle);

    //LOG_INFO << Fmt("read node success, nid=%zu", nid);
    return block;
}

void Table::async_write(nid_t nid, Block& block, Callback cb)
{
    assert(block.buffer().size() == PAGE_ROUND_UP(block.size())); 
    
    AsyncWriteContext* context = new AsyncWriteContext();

    context->nid = nid;
    context->callback = cb;
    context->handle.size = block.size();
    context->handle.offset = find_space(block.buffer().size());
    {
        ScopedMutex lock(mutex_);
        fly_writers_++;
    }

    file_->async_write(context->handle.offset, block.buffer(), 
                boost::bind(&Table::async_write_handler, this, context, _1));
}

void Table::async_write_handler(AsyncWriteContext* context, Status status)
{
    {
        ScopedMutex lock(mutex_);
        fly_writers_--;
    }

    if (status.succ) {
        ScopedMutex lock(block_entry_mutex_);

        BlockEntry::iterator iter = block_entry_.find(context->nid); 

        if (iter == block_entry_.end()) {
            BlockHandle* handle = new BlockHandle();

            *handle = context->handle;
            block_entry_[context->nid] = handle;
        } else {
            BlockHandle* handle = iter->second;

            add_fly_hole(handle->offset, PAGE_ROUND_UP(handle->size));
            *handle = context->handle;
        }
    } else {
        LOG_ERROR << "async_write error, " << Fmt("nid=%zu", context->nid);
        add_hole(context->handle.offset, context->handle.size);
    }

    context->callback(status);
    delete context;
}

uint64_t Table::find_space(uint32_t size) 
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

Block* Table::read_block(const BlockHandle* handle)
{
    Slice alloc_ptr = self_alloc(handle->size);
    if (alloc_ptr.size() == 0) {
        LOG_INFO << alloc_ptr.size();
        assert(false);
    }
    assert(alloc_ptr.size());

    if (!read_file(handle->offset, alloc_ptr)) {
        LOG_ERROR << "read_file failed";
        self_dealloc(alloc_ptr);
        return NULL;
    }

    return new Block(alloc_ptr, 0, handle->size);
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
    size_t aligned_size = PAGE_ROUND_UP(size);
    void* alloc_ptr = NULL;

    if (posix_memalign(&alloc_ptr, PAGE_SIZE, aligned_size) != 0) {
        LOG_ERROR << "posix_memalign error: " << strerror(errno);
        return Slice();
    }
    assert(alloc_ptr);
    return Slice((char*)alloc_ptr, aligned_size);
}

void Table::self_dealloc(Slice alloc_ptr)
{
    if (alloc_ptr.size())
        free((char*)alloc_ptr.data());
}
