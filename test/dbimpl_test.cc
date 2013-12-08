#include "db/db_impl.h"
#include "db/options.h"
#include "sys/thread.h"
#include "util/slice.h"
#include "util/timestamp.h"
#include <stdint.h>
#include <string.h>
#include <boost/ptr_container/ptr_vector.hpp>

using namespace yodb;

const uint64_t kCount = 200000;

DBImpl* g_db;
Options g_opts;    

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

void get_test()
{
    for (uint64_t i = 0; i < kCount; i++) {
        char buffer[16] = {0};
        sprintf(buffer, "%08ld", i);

        Slice target_value(buffer, strlen(buffer));
        Slice key = target_value;

        Slice get_value;
        g_db->get(key, get_value); 

        // assert(g_opts.comparator->compare(target_value, get_value) == 0);
        if (target_value.compare(get_value) != 0)
            printf("key=%s, expected=%s, get=%s, failed\n", 
                    key.data(), target_value.data(), get_value.data());
    }
}

int main()
{
    setenv("YODB_LOG_LEVEL", "WARN", 1);

    g_opts.comparator = new BytewiseComparator();
    g_opts.max_node_child_number = 8;
    g_opts.max_node_msg_count = 128;

    g_db = new DBImpl("test", g_opts);
    assert(g_db);

    assert(g_db->init());

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

    printf("%ld puts, cost = %f\n", 
            threads.size() * kCount, time_interval(finish, start));

    get_test();

    delete g_db;
}
