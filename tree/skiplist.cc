#include "tree/skiplist.h"

using namespace yodb;

template<typename Key, class Comparator>
class SkipList<Key, Comparator>::Node {
public:
    explicit Node(const Key& k) : key(k) { } 

    Key key;

    Node* next(size_t n) { return next_[n]; }
    Node* set_next(size_t n, Node* node) { next_[n] = node; }

private:
    Node* next_[1];
};

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* 
SkipList<Key, Comparator>::new_node(const Key& key, size_t height)
{
    size_t size = sizeof(Node) + sizeof(Node*) * (height - 1);
    char* alloc_ptr = new char[size];

    return new (alloc_ptr) Node(key);
}
