#include "fs/table.h"
#include <stdlib.h>
#include <boost/bind.hpp>

using namespace yodb;

bool Table::init(bool create)
{
    if (create) {
        if (!flush_superblock()) {
            LOG_ERROR << "flush_superblock error";
            return false;
        }
        offset_ = SUPER_BLOCK_SIZE;
        file_size_ = offset_;
    } else {
        if (file_size_ < SUPER_BLOCK_SIZE) {
            LOG_ERROR << "must be load the wrong file";
            return false;
        }
        if (!load_superblock()) {
            LOG_ERROR << "load superblock error";
            return false;
        }
        if (superblock_.index_meta.offset) {
            if (!load_index()) {
                LOG_ERROR << "load index error";
                return false;
            }
        }

        init_holes();
        LOG_INFO << block_index_.size() << " blocks found";
    }   
    truncate();
    return true;
}

void Table::init_holes()
{
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

    if (offset_set.size())
        offset_ = offset_set.rbegin()->second->offset +
            PAGE_ROUND_UP(offset_set.rbegin()->second->total_size);
    else
        offset_ = SUPER_BLOCK_SIZE;
}

void Table::add_hole(uint64_t offset, uint32_t size)
{
    Hole hole;
    hole.size = size;
    hole.offset = offset;

    if (hole_list_.empty()) {
        hole_list_.push_back(hole);
        return;
    }

    HoleList::iterator iter;
    for (iter = hole_list_.begin(); iter != hole_list_.end(); iter++) {
        if (iter->offset > hole.offset)
            break;
    }

    if (iter != hole_list_.end()) {
        assert(hole.offset + hole.size <= iter->offset);
        if (iter != hole_list_.begin())
            assert((iter-1)->offset + (iter-1)->size < hole.offset);

        if (hole.offset + hole.size == iter->offset) {
            iter->offset = hole.offset;
            iter->size += hole.size;
        } else {
            hole_list_.insert(iter, hole);
        }
    } else {
        iter--;
        if (iter->offset + iter->size == hole.offset)
            iter->size += hole.offset;
        else 
            hole_list_.push_back(hole);
    }
}

bool Table::get_hole(uint32_t size, uint64_t& offset)
{
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

void Table::truncate()
{
    if (file_size_ > offset_) {
        file_->truncate(offset_);
        file_size_ = offset_;
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
    if (writer.ok() && maybe)
        write_block_meta(superblock_.index_meta, writer);

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
               >> superblock_.index_meta.total_size;
    }

    if (!reader.ok()) 
        LOG_ERROR << "read superblock failed";

    self_dealloc(buffer);
    return reader.ok();
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

Block* Table::read(nid_t nid, bool only_index)
{
    BlockIndex::iterator iter = block_index_.find(nid); 
    assert(iter != block_index_.end());

    BlockMeta* meta = iter->second;
    assert(meta);
    Block* block = NULL;
    if (only_index)
        block = read_block(*meta, 0, meta->index_size);
    else 
        block = read_block(*meta, 0, meta->total_size); 

    LOG_INFO << Fmt("read node success, nid=%zu", nid);
    return block;
}

void Table::async_write(nid_t nid, Block& block, uint32_t index_size, Callback cb)
{
    assert(block.size() == block.buffer().size());
    assert(PAGE_ROUNDED(block.size()));
    
    AsyncWriteContext* context = new AsyncWriteContext();
    context->nid = nid;
    context->callback = cb;
    context->meta.index_size = index_size;
    context->meta.total_size = block.size();
    context->meta.offset = get_room(block.size());

    file_->async_write(context->meta.offset, block.buffer(), 
                boost::bind(&Table::async_write_handler, this, context, _1));
}

void Table::async_write_handler(AsyncWriteContext* context, Status status)
{
}

uint64_t Table::get_room(uint32_t size) 
{
    uint64_t offset;

    if (get_hole(size, offset)) 
        return offset;

    // no hole suit for us, append the file
    offset = offset_;
    offset_ += size;

    if (offset_ > file_size_)
        file_size_ = offset_;

    return offset;
}

Block* Table::read_block(BlockMeta& meta, uint32_t offset, uint32_t size)
{
    uint32_t align_offset = PAGE_ROUND_DOWN(offset);
    uint32_t fixed_size = size + (offset - align_offset);

    Slice buffer = self_alloc(fixed_size);
    if (buffer.size() == 0) {
        LOG_ERROR << "self_alloc error";
        return NULL;
    }

    if (!read_file(meta.offset + align_offset, buffer)) {
        LOG_ERROR << "read_file failed";
        self_dealloc(buffer);
        return NULL;
    }

    return new Block(buffer, offset - align_offset, size);
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
    Status stat = file_->read(offset, buffer);
    if (!stat.succ) {
        LOG_ERROR << "read file error, " << Fmt("offset=%zu.", offset);
        return false;
    }
    return true;
}

bool Table::write_file(uint64_t offset, const Slice& buffer) 
{
    Status stat = file_->write(offset, buffer);
    if (!stat.succ) {
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
