#include "tree/node.h"
#include "tree/buffer_tree.h"

using namespace yodb;

Node::Node(BufferTree* tree, nid_t self)
    : tree_(tree), self_nid_(self), parent_nid_(NID_NIL), 
      refcnt_(0), pivots_(), rwlock_(), 
      mutex_(), dirty_(false), flushing_(false)
{
}

Node::~Node()
{
    for (size_t i = 0; i < pivots_.size(); i++) {
        Pivot& pivot = pivots_[i];
        if (pivot.left_most_key.size())
            pivot.left_most_key.release();
        delete pivot.table;
    }
    pivots_.clear();
}

bool Node::get(const Slice& key, Slice& value, Node* parent)
{
    read_lock();

    if (parent)
        parent->read_unlock();

    size_t index = find_pivot(key);
    MsgTable* table = pivots_[index].table;
    assert(table);

    table->lock();
    Msg lookup;
    if (table->find(key, lookup) && lookup.key() == key) {
        if (lookup.type() == Put) {
            value = lookup.value().clone();
            table->unlock();
            read_unlock();
            return true;
        } else {
            table->unlock();
            read_unlock();
            return false;
        }
    }
    table->unlock();

    if (pivots_[index].child_nid == NID_NIL) {
        read_unlock();
        return false;
    }

    Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);
    assert(node);

    bool exists = node->get(key, value, this);
    node->dec_ref();

    return exists;
}

bool Node::write(const Msg& msg)
{
    assert(pivots_.size());

    is_leaf_ ? write_lock() : read_lock();

    if (parent_nid_ != NID_NIL) {
        is_leaf_ ? write_unlock() : read_unlock();
        return tree_->root_->write(msg);
    }

    insert_msg(find_pivot(msg.key()), msg);
    set_dirty(true);

    maybe_push_down_or_split();
    return true;
}

void Node::maybe_push_down_or_split()
{
    int index = -1;
    for (size_t i = 0; i < pivots_.size(); i++) {
        if (pivots_[i].table->count() > tree_->options_.max_node_msg_count) {
            index = i; break;
        }
    }
    if (index < 0) {
        is_leaf_ ?  write_unlock() : read_unlock();
        return;
    }
    if (pivots_[index].child_nid != NID_NIL) {
        MsgTable* table = pivots_[index].table;
        Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);
        node->push_down(table, this);
        node->dec_ref();
    } else {
        split_table(pivots_[index].table);
    }

    is_leaf_ ? write_lock() : read_lock();
    maybe_push_down_or_split();
}

void Node::create_first_pivot()
{
    write_lock();

    assert(pivots_.size() == 0);
    MsgTable* table = new MsgTable(tree_->options_.comparator); 

    pivots_.insert(pivots_.begin(), Pivot(NID_NIL, table));
    set_dirty(true);

    write_unlock();
}

size_t Node::find_pivot(Slice key)
{
    if (pivots_.size() == 0)
        return 0;

    size_t pivot = 0;
    Comparator* comp = tree_->options_.comparator;

    for (size_t i = 1; i < pivots_.size(); i++) {
        if (comp->compare(key, pivots_[i].left_most_key) < 0) {
            return pivot;
        }
        pivot++;
    }
    return pivot;
}

void Node::push_down(MsgTable* table, Node* parent)
{
    // LOG_INFO << "push_down, " << Fmt("self=%d", self_nid_);

    is_leaf_ ? write_lock() : read_lock();

    table->lock();

    if (table->count() == 0) {
        table->unlock();
        parent->read_unlock();
        is_leaf_ ? write_unlock() : read_unlock();
        return;
    }

    size_t idx = 1;
    size_t i = 0, j = 0;
    MsgTable::Iterator first(&table->list_);
    MsgTable::Iterator last(&table->list_);

    first.seek_to_first();
    last.seek_to_first();

    Comparator* cmp = tree_->options_.comparator;

    while (last.valid() && idx < pivots_.size()) {
        if (cmp->compare(last.key().key(), pivots_[idx].left_most_key) < 0) {
            j++;
            last.next();
        } else {
            while (i != j) {
                insert_msg(idx - 1, first.key());
                i++;
                first.next();
            }
            idx++;
        }
    }

    while (first.valid()) {
        insert_msg(idx - 1, first.key());
        first.next();
    }

    size_t size = table->size();
    table->clear();
    set_dirty(true);
    parent->set_dirty(true);
    assert(size >= table->size());

    table->unlock();

    assert(parent->is_leaf_ == false);
    parent->read_unlock();

    maybe_push_down_or_split();
}

void Node::split_table(MsgTable* table)
{
    assert(is_leaf_);

    if (table->count() <= tree_->options_.max_node_msg_count) {
        write_unlock();
        return;
    }

    MsgTable* src = table;
    MsgTable* dst = new MsgTable(tree_->options_.comparator);

    src->lock();

    MsgTable::Iterator iter(&src->list_);

    iter.seek_to_first();
    assert(iter.valid());
    Msg msg = iter.key();

    iter.seek_to_middle();
    assert(iter.valid());
    Msg middle = iter.key();

    dst->lock();
    while (iter.valid()) {
        dst->insert(iter.key());
        iter.next();
    }

    size_t sz = src->size();
    src->resize(src->count() / 2);

    pivots_.insert(pivots_.begin() + find_pivot(msg.key()) + 1,
                   Pivot(NID_NIL, dst, middle.key().clone()));

    assert(src->size() + dst->size() >= sz);

    dst->unlock();
    src->unlock();

    set_dirty(true);
    write_unlock();

    std::vector<Node*> locked_path;
    tree_->lock_path(msg.key(), locked_path);
     
    if (!locked_path.empty()) {
        Node* node = locked_path.back();
        node->try_split_node(locked_path);
    }
}

void Node::try_split_node(std::vector<Node*>& path)
{
    assert(path.back() == this);

    if (pivots_.size() <= tree_->options_.max_node_child_number) {
        while (!path.empty()) {
            Node* node = path.back();
            node->write_unlock();
            node->dec_ref();
            path.pop_back();
        }
        return;
    }

    size_t half = pivots_.size() / 2;
    Slice half_key = pivots_[half].left_most_key;
    size_t half_size = 0;

    // LOG_INFO << "try_split_node, " << half_key.data();

    // TODO: consider remove parent_nid_ class member
    Node* node = tree_->create_node();
    node->is_leaf_ = is_leaf_;
    node->parent_nid_ = parent_nid_;
    
    Container::iterator first = pivots_.begin() + half;
    Container::iterator last  = pivots_.end();

    for (Container::iterator iter = first; iter != last; iter++) {
        if (iter->child_nid != NID_NIL) {
            Node* child = tree_->get_node_by_nid(iter->child_nid);
            child->parent_nid_ = node->self_nid_;
            child->dec_ref();
        }

        half_size += iter->table->size();
    }
    node->pivots_.insert(node->pivots_.begin(), first, last);
    node->set_dirty(true);
    node->dec_ref();

    pivots_.resize(half); 
    set_dirty(true);

    path.pop_back();
    write_unlock();
    dec_ref();

    if (path.empty()) {
        Node* root = tree_->create_node();
        root->is_leaf_ = false;

        parent_nid_ = root->self_nid_;
        node->parent_nid_ = root->self_nid_;

        MsgTable* left = new MsgTable(tree_->options_.comparator);
        MsgTable* right = new MsgTable(tree_->options_.comparator);

        root->pivots_.reserve(2);
        root->pivots_.insert(root->pivots_.begin(), 
                             Pivot(node->self_nid_, right, half_key.clone()));
        root->pivots_.insert(root->pivots_.begin(), 
                             Pivot(self_nid_, left));

        root->set_dirty(true);
        tree_->grow_up(root);
    } else {
        Node* parent = path.back();

        parent->add_pivot(half_key, node->self_nid_);
        parent->try_split_node(path);
    }
}

void Node::add_pivot(Slice key, nid_t child)
{
    MsgTable* table = new MsgTable(tree_->options_.comparator);
    size_t pivot_index = find_pivot(key);

    pivots_.insert(pivots_.begin() + pivot_index + 1, 
                   Pivot(child, table, key.clone()));

    set_dirty(true);
}

void Node::lock_path(const Slice& key, std::vector<Node*>& path)
{
    path.push_back(this);

    size_t index = find_pivot(key);

    if (pivots_[index].child_nid != NID_NIL) {
        Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);

        node->write_lock();
        node->push_down_locked(pivots_[index].table, this);
        assert(node->parent_nid_ == path.back()->self_nid_);
        node->lock_path(key, path);
    }
}

void Node::push_down_locked(MsgTable* table, Node* parent)
{
    table->lock();

    if (table->count() == 0) {
        table->unlock();
        return;
    }

    size_t idx = 1;
    size_t i = 0, j = 0;
    MsgTable::Iterator first(&table->list_);
    MsgTable::Iterator last(&table->list_);

    first.seek_to_first();
    last.seek_to_first();

    Comparator* cmp = tree_->options_.comparator;

    while (last.valid() && idx < pivots_.size()) {
        if (cmp->compare(last.key().key(), pivots_[idx].left_most_key) < 0) {
            j++;
            last.next();
        } else {
            while (i != j) {
                insert_msg(idx - 1, first.key());
                i++;
                first.next();
            }
            idx++;
        }
    }

    while (first.valid()) {
        insert_msg(idx - 1, first.key());
        first.next();
    }

    set_dirty(true);
    parent->set_dirty(true);
    table->clear();
    table->unlock();
}

void Node::insert_msg(size_t index, const Msg& msg)
{
    MsgTable* table = pivots_[index].table;
    
    table->lock();
    table->insert(msg);
    table->unlock();
}

size_t Node::size()
{
    size_t usage = sizeof(Node);

    for (size_t i = 0; i < pivots_.size(); i++)
        usage += pivots_[i].table->memory_usage() + pivots_[i].table->size();

    usage += pivots_.size() * sizeof(Pivot);

    return usage;
}

size_t Node::write_back_size()
{
    size_t size = 0;

    size += 8;      // self_nid_
    size += 8;      // parent_nid_
    size += 1;      // is_leaf
    size += 4;      // number of pivots

    for (size_t i = 0; i < pivots_.size(); i++) {
        size += 8;                                      // child 
        size += 4 + pivots_[i].left_most_key.size();    // left_most_key
        size += pivots_[i].table->size();               // table size
    }

    return size;
}

bool Node::constrcutor(BlockReader& reader)
{
    reader >> self_nid_ >> parent_nid_ >> is_leaf_;

    uint32_t pivots = 0;
    reader >> pivots;

    assert(reader.ok());
    assert(pivots > 0);

    pivots_.reserve(pivots);

    for (size_t i = 0; i < pivots; i++) {
        nid_t child;
        MsgTable* table = new MsgTable(tree_->options_.comparator);
        Slice left_most_key;

        reader >> child >> left_most_key;
        table->constrcutor(reader);

        pivots_.push_back(Pivot(child, table, left_most_key));
    }
    set_dirty(true);

    return reader.ok();
}

bool Node::destructor(BlockWriter& writer)
{
    writer << self_nid_ << parent_nid_ << is_leaf_;

    uint32_t pivots = pivots_.size();
    assert(pivots > 0);

    writer << pivots;

    for (size_t i = 0; i < pivots; i++) {
        writer << pivots_[i].child_nid
               << pivots_[i].left_most_key;
        pivots_[i].table->destructor(writer);
    }

    return writer.ok();
}

void Node::set_dirty(bool dirty)
{
    ScopedMutex lock(mutex_);

    if (!dirty_ && dirty) 
        first_write_timestamp_ = Timestamp::now();

    dirty_ = dirty;
}

bool Node::dirty() 
{
    ScopedMutex lock(mutex_);
    return dirty_;
}

void Node::set_flushing(bool flushing)
{
    ScopedMutex lock(mutex_);
    flushing_ = flushing;
}

bool Node::flushing()
{
    ScopedMutex lock(mutex_);
    return flushing_;
}

void Node::inc_ref()
{
    ScopedMutex lock(mutex_);
    refcnt_++;
}

void Node::dec_ref()
{
    ScopedMutex lock(mutex_);
    assert(refcnt_ > 0);

    refcnt_--;
    last_used_timestamp_ = Timestamp::now();
}

size_t Node::refs()
{
    ScopedMutex lock(mutex_);
    return refcnt_;
}

Timestamp Node::get_first_write_timestamp()
{
    ScopedMutex lock(mutex_);
    return first_write_timestamp_;
}

Timestamp Node::get_last_used_timestamp()
{
    ScopedMutex lock(mutex_);
    return last_used_timestamp_;
}
