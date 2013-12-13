#include "db/db_impl.h"
#include "db/options.h"
#include "sys/thread.h"
#include "util/slice.h"
#include "util/timestamp.h"
#include "util/logger.h"
#include <stdint.h>
#include <string.h>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/bind.hpp>

using namespace yodb;

const uint64_t kCount = 10 * 1000 * 1000;

DBImpl* g_db;

void thr_put1()
{
    for (uint64_t i = 0; i < kCount; i++) {
        char buffer[16] = {0};
        sprintf(buffer, "%08ld", i);

        Slice value(buffer, strlen(buffer));
        Slice key = value;

        g_db->put(key, value); 
    } 
}

void thr_put2()
{
    for (uint64_t i = kCount; i < kCount * 2; i++) {
        char buffer[16] = {0};
        sprintf(buffer, "%08ld", i);

        Slice value(buffer, strlen(buffer));
        Slice key = value;

        g_db->put(key, value); 
    } 
}

void thr_put3()
{
    for (uint64_t i = kCount * 2; i < kCount * 3; i++) {
        char buffer[16] = {0};
        sprintf(buffer, "%08ld", i);

        Slice value(buffer, strlen(buffer));
        Slice key = value;

        g_db->put(key, value); 
    } 
}

void thr_put4()
{
    for (uint64_t i = kCount * 3; i < kCount * 4; i++) {
        char buffer[16] = {0};
        sprintf(buffer, "%08ld", i);

        Slice value(buffer, strlen(buffer));
        Slice key = value;

        g_db->put(key, value); 
    } 
}

void get_thr(uint64_t x, uint64_t y)
{
    uint64_t count = 0;
    for (uint64_t i = x; i < y; i++) {
        char buffer[16] = {0};
        sprintf(buffer, "%08ld", i);

        Slice target_value(buffer, strlen(buffer));
        Slice key = target_value;

        Slice get_value;
        g_db->get(key, get_value); 

        if (target_value.compare(get_value) != 0)
            count++;

        if (get_value.size())
            get_value.release();
    }

    LOG_INFO << Fmt("read %zu records, ", y - x) << Fmt("%zu failed", count);
}

int main()
{
    Options opts;
    opts.comparator = new BytewiseComparator();
    opts.max_node_child_number = 16;
    opts.max_node_msg_count = 10240;
    opts.cache_limited_memory = 1 << 28;
    opts.env = new Env("/home/kedebug/develop/yodb/bin");

    g_db = new DBImpl("third", opts);
    g_db->init();

    boost::ptr_vector<Thread> threads;
    threads.push_back(new Thread(&thr_put1));
    threads.push_back(new Thread(&thr_put2));
    threads.push_back(new Thread(&thr_put3));
    threads.push_back(new Thread(&thr_put4));

    Timestamp start, finish;

    start = Timestamp::now();
    for (size_t i = 0; i < threads.size(); i++)
        threads[i].run();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i].join();
    finish = Timestamp::now();

    LOG_INFO << Fmt("%ld puts, ", threads.size() * kCount)
             << Fmt("cost = %f", time_interval(finish, start));

    threads.clear();
    delete g_db;

    g_db = new DBImpl("third", opts);
    g_db->init();

    threads.push_back(new Thread(boost::bind(&get_thr, 0, kCount)));
    threads.push_back(new Thread(boost::bind(&get_thr, kCount, kCount * 2)));
    threads.push_back(new Thread(boost::bind(&get_thr, kCount * 2, kCount * 3)));
    threads.push_back(new Thread(boost::bind(&get_thr, kCount * 3, kCount * 4)));

    start = Timestamp::now();
    for (size_t i = 0; i < threads.size(); i++)
        threads[i].run();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i].join();
    finish = Timestamp::now();

    LOG_INFO << Fmt("%ld get, ", threads.size() * kCount)
             << Fmt("cost = %f", time_interval(finish, start));

    delete g_db;
    delete opts.env;
    delete opts.comparator;
}
