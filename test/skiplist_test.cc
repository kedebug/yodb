#include "util/logger.h"
#include "tree/skiplist.h"
#include <vector>

using namespace yodb;

typedef uint64_t Key;

struct Comparator {
    int operator()(const Key& a, const Key& b) const 
    {
        if (a < b) return -1;
        else if (a > b) return 1;
        else return 0;
    }
};

void test_empty()
{
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
    assert(!list.contains(10));

    SkipList<Key, Comparator>::Iterator iter(&list);

    assert(!iter.valid());
    iter.seek_to_first();
    assert(!iter.valid());
    iter.seek_to_middle();
    assert(!iter.valid());
    iter.seek_to_last();
    assert(!iter.valid());
}

void test_insert_erase()
{
    const size_t N = 100;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);

    for (size_t i = 0; i < N; i += 2)
        list.insert(i);
    for (size_t i = 1; i < N; i += 2)
        list.insert(i);

    // test repeat
    for (size_t i = 0; i < N; i += 2)
        list.insert(i);
    for (size_t i = 1; i < N; i += 2)
        list.insert(i);

    assert(list.count() == N);

    SkipList<Key, Comparator>::Iterator iter(&list);

    iter.seek_to_first();
    assert(cmp(iter.key(), 0) == 0);
    iter.seek_to_middle();
    assert(cmp(iter.key(), 50) == 0);

    list.erase(50);
    iter.seek_to_middle();
    assert(cmp(iter.key(), 49) == 0);
    
    list.insert(50);
    iter.seek_to_middle();
    assert(cmp(iter.key(), 50) == 0);

    std::vector<Key> v1, v2;

    iter.seek_to_first();
    while (iter.valid()) {
        v1.push_back(iter.key());
        iter.next();
    }

    iter.seek_to_last();
    while (iter.valid()) {
        v2.push_back(iter.key());
        iter.prev();
    }

    assert(v1.size() == v2.size());
    size_t size = v1.size();
    for (size_t i = 0; i < size; i++)
        assert(v1[i] == v2[size-1-i]);

    // test resize
    
    LOG_INFO << Fmt("before memory usage=%zu", list.memory_usage());

    assert(list.count() == N); 
    list.resize(N / 2);

    LOG_INFO << Fmt("after memory usage=%zu", list.memory_usage());
    
    iter.seek_to_first();
    for (size_t i = 0; i < list.count(); i++) {
        assert(iter.key() == i);
        iter.next();
    }
}

int main()
{
    test_empty();
    test_insert_erase();
}
