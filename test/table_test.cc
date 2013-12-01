#include "fs/file.h"
#include "fs/table.h"
#include "fs/env.h"
#include "sys/mutex.h"
#include "sys/thread.h"

#include <stdlib.h>
#include <map>
#include <boost/bind.hpp>

using namespace yodb;

const size_t min_page_size = 1;
const size_t max_page_size = 32 * 1024;
const uint64_t kMaxCount = 1000;

Table* table;
std::map<nid_t, bool> g_result;
std::map<nid_t, Block*> g_blocks; 
Mutex g_result_mutex;
Mutex g_blocks_mutex;

void async_write_handler(nid_t nid, Status status)
{
    ScopedMutex lock(g_result_mutex);
    g_result[nid] = status.succ;
}

void async_write_test(size_t count)
{
    srand(0);
    for (nid_t i = kMaxCount * count; i < kMaxCount * (count + 1); i++) {
        size_t size = min_page_size + rand() % (max_page_size - min_page_size);
        Slice buffer = table->self_alloc(size);
        Block* block = new Block(buffer, 0, size);

        {
            ScopedMutex lock(g_blocks_mutex);
            g_blocks[i] = block;
        }
        BlockWriter writer(*block);

        for (size_t j = 0; j < size; j++)
            writer << (uint8_t)(i & 0xFF);

        table->async_write(i, *block, size, boost::bind(&async_write_handler, i, _1));
    } 
}

void read_test(size_t n)
{
    for (nid_t i = 0; i < n; i++) {
        Block* block = table->read(i);
        assert(block);
        assert(g_blocks[i]->size() == block->size());
        assert(memcmp(g_blocks[i]->data(), block->data(), block->size()) == 0);
        table->self_dealloc(block->buffer());
    }
}

void release()
{
    std::map<nid_t, Block*>::iterator iter;
    for (iter = g_blocks.begin(); iter != g_blocks.end(); iter++) {
        table->self_dealloc(iter->second->buffer());
        delete iter->second;
    }
    g_blocks.clear();
}

int main()
{
    Env env("./");
    AIOFile* file = env.open_aio_file("table_test");

    table = new Table(file, 0);
    table->init(true);

    /////////////////////////////////////////////////////
    // 1st test
    async_write_test(0);
    while (g_result.size() != kMaxCount)
        usleep(1000);

    for (size_t i = 0; i < g_result.size(); i++)
        assert(g_result[i]);

    uint64_t file_size = table->size();
    delete table;

    table = new Table(file, file_size);
    table->init(false);
    read_test(kMaxCount);

    /////////////////////////////////////////////////////
    // 2nd test
    release();
    
    g_result.clear();

    async_write_test(0);
    while (g_result.size() != kMaxCount)
        usleep(1000);

    for (size_t i = 0; i < g_result.size(); i++)
        assert(g_result[i]);

    table->flush_right_now();
    read_test(kMaxCount);
    
    file_size = table->size();
    release();
    delete table;

    /////////////////////////////////////////////////////
    // multithreading environment test
    table = new Table(file, file_size);
    table->init(false);

    Thread thr1(boost::bind(&async_write_test, 0));
    Thread thr2(boost::bind(&async_write_test, 1));

    thr1.run();
    thr2.run();

    table->flush_right_now();
    thr1.join();
    thr2.join();

    while (g_result.size() != 2 * kMaxCount)
        usleep(1000);

    for (size_t i = 0; i < g_result.size(); i++)
        assert(g_result[i]);

    read_test(2 * kMaxCount); 

    release();
    delete table;
}
