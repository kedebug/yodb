#include "tree/node.h"
#include "tree/buffer_tree.h"

using namespace yodb;

bool Node::get(const Slice& key, Slice& value)
{
    read_lock();

    size_t index = find_pivot(key);
    MsgBuf* msgbuf = pivots_[index].msgbuf;
    assert(msgbuf);

    msgbuf->lock();
    MsgBuf::Iterator iter = msgbuf->find(key);
    if (iter != msgbuf->end() && iter->key() == key) {
        if (iter->type() == Put) {
            value = iter->value().clone();
            msgbuf->unlock();
            read_unlock();
            return true;
        } else {
            msgbuf->unlock();
            read_unlock();
            return false;
        }
    }
    msgbuf->unlock();

    if (pivots_[index].child_nid == NID_NIL) {
        read_unlock();
        return false;
    }

    Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);
    assert(node);

    read_unlock();
    return node->get(key, value);
}

bool Node::write(const Msg& msg)
{
    assert(pivots_.size());

    is_leaf_ ? write_lock() : read_lock();

    if (parent_nid_ != NID_NIL) {
        is_leaf_ ? write_unlock() : read_unlock();
        return tree_->root_->write(msg);
    }
    size_t pivot_index = find_pivot(msg.key());

    MsgBuf* msgbuf = pivots_[pivot_index].msgbuf;
    msgbuf->insert(msg);

    maybe_push_down_or_split();
    return true;
}

void Node::maybe_split_msgbuf()
{
    
}

void Node::maybe_push_down_or_split()
{
    int index = -1;
    for (size_t i = 0; i < pivots_.size(); i++) {
        if (pivots_[i].msgbuf->msg_count() > tree_->options_.max_node_msg_count) {
            index = i; break;
        }
    }
    if (index < 0) {
        is_leaf_ ?  write_unlock() : read_unlock();
        return;
    }
    if (pivots_[index].child_nid != NID_NIL) {
        MsgBuf* msgbuf = pivots_[index].msgbuf;
        Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);
        node->push_down_msgbuf(msgbuf, self_nid_);
    } else {
        split_msgbuf(pivots_[index].msgbuf);
    }

    is_leaf_ ? write_lock() : read_lock();
    maybe_push_down_or_split();
}

void Node::create_first_pivot()
{
    write_lock();
    assert(pivots_.size() == 0);
    MsgBuf* msgbuf = new MsgBuf(tree_->options_.comparator); 
    pivots_.insert(pivots_.begin(), Pivot(NID_NIL, msgbuf));
    write_unlock();
}

size_t Node::find_pivot(Slice key)
{
    if (pivots_.size() == 0)
        return 0;

    size_t pivot = 0;
    Comparator* comp = tree_->options_.comparator;

    for (size_t i = 1; i < pivots_.size(); i++) {
        if (comp->compare(key, pivots_[i].left_most_key()) < 0) {
            return pivot;
        }
        pivot++;
    }
    return pivot;
}

void Node::push_down_msgbuf(MsgBuf* msgbuf, nid_t parent_nid)
{
    //LOG_INFO << "push_down_msgbuf, " << Fmt("self=%d", self_nid_);

    is_leaf_ ? write_lock() : read_lock();

    msgbuf->lock();

    Node* parent = tree_->get_node_by_nid(parent_nid);

    size_t index = 1;
    MsgBuf::Iterator first = msgbuf->begin();
    MsgBuf::Iterator last  = msgbuf->begin();
    Comparator* comp = tree_->options_.comparator;

    while (last != msgbuf->end() && index < pivots_.size()) {
        if (comp->compare(last->key(), pivots_[index].left_most_key()) < 0) {
            last++;
        } else {
            while (first != last) {
                pivots_[index-1].msgbuf->insert(*first);
                first++;
            }
            index++;
        }
    }
    while (first != msgbuf->end()) {
        pivots_[index-1].msgbuf->insert(*first);
        first++;
    }
    msgbuf->release();
    msgbuf->unlock();
    
    assert(parent->is_leaf_ == false);
    parent->read_unlock();

    maybe_push_down_or_split();
}

void Node::split_msgbuf(MsgBuf* msgbuf)
{
    assert(is_leaf_);

    if (msgbuf->msg_count() <= tree_->options_.max_node_msg_count) {
        write_unlock();
        return;
    }

    MsgBuf* srcbuf = msgbuf;
    MsgBuf* dstbuf = new MsgBuf(tree_->options_.comparator);    
    
    size_t half = srcbuf->msg_count() / 2;

    srcbuf->lock();

    // for (MsgBuf::Iterator it = srcbuf->begin(); it != srcbuf->end(); it++)
    //     LOG_INFO << "before split_msgbuf, " << it->key().data();

    Slice half_key = (srcbuf->begin() + half)->key();
    Slice key = srcbuf->begin()->key();

    MsgBuf::Iterator first = srcbuf->begin() + half;
    MsgBuf::Iterator last  = srcbuf->end();

    dstbuf->insert(dstbuf->begin(), first, last);

    srcbuf->resize(half);
    srcbuf->unlock();

    pivots_.insert(pivots_.begin() + find_pivot(key) + 1, 
                   Pivot(NID_NIL, dstbuf, half_key.clone()));

    // LOG_INFO << "split_msgbuf, " 
    //          << Fmt("%d ", half) << Fmt("%d ", dstbuf->msg_count())
    //          << half_key.data();

    write_unlock();

    std::vector<nid_t> locked_path;
    tree_->lock_path(key, locked_path);

     
    // LOG_INFO << locked_path.back() << ' ' << self_nid_;
    // assert(locked_path.back() == self_nid_);
    // if (locked_path.back() != self_nid_) {
    //     for (size_t i = 0; i < locked_path.size(); i++)
    //         LOG_INFO << locked_path[i];
    //     //assert(false);
    // }

    if (!locked_path.empty()) {
        Node* node = tree_->get_node_by_nid(locked_path.back());
        node->try_split_node(locked_path);
    }
}

void Node::try_split_node(std::vector<nid_t>& path)
{
    assert(path.back() == self_nid_);

    if (pivots_.size() <= tree_->options_.max_node_child_number) {
        while (!path.empty()) {
            Node* node = tree_->get_node_by_nid(path.back());
            node->write_unlock();
            path.pop_back();
        }
        return;
    }

    size_t half = pivots_.size() / 2;
    Slice half_key = pivots_[half].left_most_key();

    // LOG_INFO << "try_split_node, " << half_key.data();

    Node* node = tree_->create_node(is_leaf_);
    node->parent_nid_ = parent_nid_;
    
    Container::iterator first = pivots_.begin() + half;
    Container::iterator last  = pivots_.end();

    for (Container::iterator iter = first; iter != last; iter++) {
        if (iter->child_nid == NID_NIL) 
            break;
        Node* n = tree_->get_node_by_nid(iter->child_nid);
        n->parent_nid_ = node->self_nid_;
    }
    node->pivots_.insert(node->pivots_.begin(), first, last);
    pivots_.resize(half); 


    if (parent_nid_ == NID_NIL) {
        // assert(path.empty());
        Node* root = tree_->create_node(false);

        parent_nid_ = root->self_nid_;
        node->parent_nid_ = root->self_nid_;

        MsgBuf* left = new MsgBuf(tree_->options_.comparator);
        MsgBuf* right = new MsgBuf(tree_->options_.comparator);

        root->pivots_.reserve(2);
        root->pivots_.insert(root->pivots_.begin(), 
                             Pivot(node->self_nid_, right, half_key.clone()));
        root->pivots_.insert(root->pivots_.begin(), 
                             Pivot(self_nid_, left));

        tree_->grow_up(root);
        // LOG_INFO << "grow_up: " << root->self_nid_;

        path.pop_back();
        write_unlock();
        assert(path.empty());
    } else {
        path.pop_back();
        write_unlock();

        // LOG_INFO << Fmt("%ld parent ", self_nid_) << parent_nid_;
        Node* parent = tree_->get_node_by_nid(parent_nid_);
        assert(parent);
        parent->add_pivot(half_key, node->self_nid_, self_nid_);
        parent->try_split_node(path);
    }
}

void Node::add_pivot(Slice key, nid_t child, nid_t child_sibling)
{
    MsgBuf* msgbuf = new MsgBuf(tree_->options_.comparator);

    // for (size_t i = 0; i < pivots_.size(); i++) {
    //     if (pivots_[i].child_nid == child_sibling) {
    //         pivots_.insert(pivots_.begin() + i + 1,
    //                        Pivot(child, msgbuf, key.clone()));
    //         break;
    //     }
    // }
    size_t pivot_index = find_pivot(key);

    pivots_.insert(pivots_.begin() + pivot_index + 1, 
                   Pivot(child, msgbuf, key.clone()));
}

void Node::lock_path(const Slice& key, std::vector<nid_t>& path)
{
    path.push_back(self_nid_);

    size_t index = find_pivot(key);

    if (pivots_[index].child_nid != NID_NIL) {
        Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);
        node->write_lock();
        node->push_down_during_lock_path(pivots_[index].msgbuf);
        assert(node->parent_nid_ == path.back());
        node->lock_path(key, path);
    } else {
        assert(is_leaf_);
        //read_unlock();
        //write_lock();
    }
}

void Node::push_down_during_lock_path(MsgBuf* msgbuf)
{
    msgbuf->lock();

    size_t index = 1;
    MsgBuf::Iterator first = msgbuf->begin();
    MsgBuf::Iterator last  = msgbuf->begin();
    Comparator* comp = tree_->options_.comparator;

    while (last != msgbuf->end() && index < pivots_.size()) {
        if (comp->compare(last->key(), pivots_[index].left_most_key()) < 0) {
            last++;
        } else {
            while (first != last) {
                // LOG_INFO << "push_down_during_lock_path, msg, " 
                //          << first->key().data();
                pivots_[index-1].msgbuf->insert(*first);
                first++;
            }
            index++;
        }
    }
    while (first != msgbuf->end()) {
        // LOG_INFO << "push_down_during_lock_path, msg, " << first->key().data();
        pivots_[index-1].msgbuf->insert(*first);
        first++;
    }
    msgbuf->release();
    msgbuf->unlock();
}
