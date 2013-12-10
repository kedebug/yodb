#include "util/arena.h"
#include <assert.h>

using namespace yodb;

Arena::Arena()
{
    blocks_size_ = 0;
    alloc_ptr_ = NULL;
    remaining_ = 0;
}

Arena::~Arena()
{
    clear();
}

char* Arena::alloc(size_t bytes)
{
    assert(bytes > 0);

    if (bytes <= remaining_) {
        char* result = alloc_ptr_;

        alloc_ptr_ += bytes;
        remaining_ -= bytes;
        
        return result;
    }

    return alloc_fallback(bytes);
}

char* Arena::alloc_aligned(size_t bytes)
{
    size_t cut = reinterpret_cast<uintptr_t>(alloc_ptr_) & (kAlignedSize - 1); 
    size_t slop = (cut == 0 ? 0 : kAlignedSize - cut);
    size_t fixed_size = bytes + slop;
    char* result;

    if (fixed_size <= remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += fixed_size;
        remaining_ -= fixed_size;
    } else {
        result = alloc_fallback(bytes);
    }

    assert((reinterpret_cast<uintptr_t>(result) & (kAlignedSize - 1)) == 0);

    return result;
}

char* Arena::alloc_fallback(size_t bytes)
{
    if (bytes > kBlockSize / 4)
        return alloc_new_block(bytes);

    alloc_ptr_ = alloc_new_block(kBlockSize);
    remaining_ = kBlockSize;

    char* result = alloc_ptr_;

    alloc_ptr_ += bytes;
    remaining_ -= bytes;

    return result;
}

char* Arena::alloc_new_block(size_t block_bytes)
{
    char* result = new char[block_bytes];

    blocks_size_ += block_bytes;
    blocks_.push_back(result);

    return result;
}

size_t Arena::usage() const 
{
    return blocks_size_ + blocks_.capacity() * (sizeof(char*));
}

void Arena::clear()
{
    for (size_t i = 0; i < blocks_.size(); i++)
        delete[] blocks_[i];

    blocks_size_ = 0;
    alloc_ptr_ = NULL;
    remaining_ = 0;

    blocks_.clear();
}
