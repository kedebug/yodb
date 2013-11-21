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
    read_lock();

    size_t pivot_index = find_pivot(msg.key());

    if (pivot_index >= pivots_.size()) {
        // this would happen only when the size of pivots_ is 0
        read_unlock();
        create_first_pivot();
        read_lock();
    }

    MsgBuf* msgbuf = pivots_[pivot_index].msgbuf;

    // considing multithreads come to insert msg to same msgbuf
    msgbuf->insert(msg);

    read_unlock();

    if (msgbuf->msg_count() > tree_->options_.max_node_msg_count) {
        if (pivots_[pivot_index].child_nid == NID_NIL)
            split_msgbuf(pivot_index);
        else
            push_down_msgbuf(pivot_index);
    } 

    return true;
}

bool Node::special_write_during_push_down(const Msg& msg)
{
    //read_lock();

    size_t pivot_index = find_pivot(msg.key());
    MsgBuf* msgbuf = pivots_[pivot_index].msgbuf;

    msgbuf->insert(msg);

    //read_unlock();
    return true;
}

void Node::create_first_pivot()
{
    write_lock();
    if (pivots_.size() == 0) {
        MsgBuf* msgbuf = new MsgBuf(tree_->options_.comparator); 
        pivots_.insert(pivots_.begin(), Pivot(NID_NIL, msgbuf));
    }
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

void Node::push_down_msgbuf(size_t pivot_index)
{
    read_lock();

    nid_t child = pivots_[pivot_index].child_nid;
    assert(child != NID_NIL); 

    Node* node = tree_->get_node_by_nid(child);
    assert(node);
    
    MsgBuf* msgbuf = pivots_[pivot_index].msgbuf;

    read_unlock();
    msgbuf->lock();
    for (MsgBuf::Iterator it = msgbuf->begin(); it != msgbuf->end(); it++) {
        // TODO: optimization
        // warning: node mustn't split during push_down_msgbuf() 
        //if (node->pivots_[0].child_nid == NID_NIL)
        //    node->special_write_during_push_down(*it);
        //else
            node->write(*it);
    }
    msgbuf->unlock();
    msgbuf->release();
}

void Node::split_msgbuf(size_t index)
{
    write_lock();

    if (pivots_[index].msgbuf->msg_count() < tree_->options_.max_node_msg_count) {
        write_unlock();
        return;
    }

    MsgBuf* srcbuf = pivots_[index].msgbuf;
    MsgBuf* dstbuf = new MsgBuf(tree_->options_.comparator);    
    
    size_t half = srcbuf->msg_count() / 2;
    Slice key = (srcbuf->begin() + half)->key();

    MsgBuf::Iterator first = srcbuf->begin() + half;
    MsgBuf::Iterator last  = srcbuf->end();

    dstbuf->insert(dstbuf->begin(), first, last);

    LOG_INFO << "split_msgbuf, " << half << ' ' << srcbuf->msg_count() << ' ' << key.data();

    srcbuf->resize(half);

    pivots_.insert(pivots_.begin() + index + 1, 
                   Pivot(NID_NIL, dstbuf, key.clone()));

    write_unlock();
 
    // rarely concurrency can come here, here all lock is free
    try_split_node();
}

void Node::try_split_node()
{
    if (pivots_.size() <= tree_->options_.max_node_child_number) return;

    write_lock();

    size_t half = pivots_.size() / 2;
    Slice half_key = pivots_[half].left_most_key();

    LOG_INFO << "try_split_node, " << half_key.data();

    Node* node = tree_->create_node(parent_nid_);
    
    Container::iterator first = pivots_.begin() + half;
    Container::iterator last  = pivots_.end();

    node->pivots_.insert(node->pivots_.begin(), first, last);
    pivots_.resize(half); 

    if (parent_nid_ == NID_NIL) {
        LOG_INFO << "grow_up";
        Node* root = tree_->create_node(NID_NIL);

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
    } else {
        Node* parent = tree_->get_node_by_nid(parent_nid_);
        assert(parent);
        parent->add_pivot(half_key, node->self_nid_, self_nid_);
    }

    write_unlock();
}

void Node::add_pivot(Slice key, nid_t child, nid_t child_sibling)
{
    // write lock the node when add a pivot
    write_lock();

    MsgBuf* msgbuf = new MsgBuf(tree_->options_.comparator);

    for (size_t i = 0; i < pivots_.size(); i++) {
        if (pivots_[i].child_nid == child_sibling) {
            pivots_.insert(pivots_.begin() + i + 1,
                           Pivot(child, msgbuf, key.clone()));
            break;
        }
    }

    //size_t pivot_index = find_pivot(key);

    //pivots_.insert(pivots_.begin() + pivot_index + 1, 
    //               Pivot(child, msgbuf, key.clone()));

    write_unlock();

    try_split_node();
}
