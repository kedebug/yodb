#include "fs/file.h"
#include "fs/env.h"
#include "sys/mutex.h"
#include "util/slice.h"

#include <stdlib.h>
#include <malloc.h>
#include <unistd.h>

#include <map>
#include <boost/bind.hpp>

using namespace yodb;

const size_t kMaxCount = 2000;
const size_t kBoundarySize = 4096;
const size_t kBlockSize = 4096;

Slice alloc_aligned_buffer(size_t size)
{
    void* ptr = memalign(kBoundarySize, size);
    return Slice((char*)ptr, size);
}

void free_aligned_buffer(Slice& buffer)
{
    free((void*)buffer.data());
}

Mutex g_mutex;
std::map<int, Status> g_map;

void complete(int* id, Status stat)
{
    ScopedMutex lock(g_mutex);
    g_map[*id] = stat;
}

int main()
{
    Env env("./");    
    AIOFile* file = env.open_aio_file("aio_file_test");
    assert(file);
    
    int id[kMaxCount];
    Slice buffer[kMaxCount];

    for (size_t i = 0; i < kMaxCount; i++) {
        id[i] = i;
        buffer[i] = alloc_aligned_buffer(kBlockSize);
        memset((void*)buffer[i].data(), i & 0xFF, kBlockSize);

        file->async_write(i * kBlockSize, buffer[i], 
                boost::bind(&complete, id + i, _1));
    }

    while (g_map.size() != kMaxCount)
        usleep(10000);

    for (size_t i = 0; i < kMaxCount; i++) {
        assert(g_map[i].succ);
        assert(g_map[i].size == kBlockSize);
    }

    g_map.clear();

    for (size_t i = 0; i < kMaxCount; i++) {
        id[i] = i;
        file->async_read(i * kBlockSize, buffer[i], 
                boost::bind(&complete, id + i, _1));
    }

    while (g_map.size() != kMaxCount)
        usleep(10000);

    for (size_t i = 0; i < kMaxCount; i++) {
        assert(g_map[i].succ);
        assert(g_map[i].size == kBlockSize);

        char dst[kBlockSize];
        memset((void*)dst, i & 0xFF, kBlockSize);
        assert(memcmp(buffer[i].data(), dst, kBlockSize) == 0);

        free_aligned_buffer(buffer[i]);
    }
    
}
