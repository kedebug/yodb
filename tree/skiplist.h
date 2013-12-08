#ifndef _YODB_SKIPLIST_H_
#define _YODB_SKIPLIST_H_

#include <boost/noncopyable.hpp>

namespace yodb {

template<typename Key, class Comparator>
class SkipList {
private:
    class Node;
public:
    explicit SkipList(Comparator cmp);
    
    void insert(const Key& key);
    bool contains(const Key& key) const;
    void erase(const Key& key);

    class Iterator {
    public:
        explicit Iterator(const SkipList* list);

        bool valid() const;
        const Key& key() const;
        void next();
        void prev();

    private:
        SkipList* list_;
        Node* node_;
    };

private:
    enum { kMaxHeight = 11 };
    
    Node* const head_;
    size_t max_height_;
    Comparator compare_;

    bool equal(const Key& a, const Key& b);

    Node* new_node(const Key& key, size_t height);
    Node* find_greater_or_equal(const Key& key, Node** prev) const;
    Node* find_less_than(const Key& key) const;
};

} // namespace yodb

#endif // _YODB_SKIPLIST_H_
