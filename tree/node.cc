#include "tree/node.h"
#include "tree/buffer_tree.h"

using namespace yodb;

bool Node::get(Slice key, Slice& value)
{
    size_t index = find_pivot(key);
    MsgBuf* msgbuf = pivots_[index].msgbuf;
    assert(msgbuf);

    MsgBuf::Iterator iter = msgbuf->find(key);
    if (iter != msgbuf->end() && iter->key() == key) {
        if (iter->type() == Put) {
            value = iter->value().clone();
            return true;
        } else {
            return false;
        }
    }

    Node* node = tree_->get_node_by_nid(pivots_[index].child_nid);
    assert(node);
    return node->get(key, value);
}

bool Node::write(const Msg& msg)
{
    size_t pivot_index = find_pivot(msg.key());

    if (pivot_index >= pivots_.size()) {
        // this would happen only when the size of pivots_ is 0
        MsgBuf* msgbuf = new MsgBuf(tree_->options_.comparator); 
        pivots_.insert(pivots_.begin(), Pivot(NID_NIL, msgbuf));
    }

    MsgBuf* msgbuf = pivots_[pivot_index].msgbuf;
    msgbuf->insert(msg);

    if (msgbuf->msg_count() > tree_->options_.max_node_msg_count) {
        if (pivots_[pivot_index].child_nid == NID_NIL)
            split_msgbuf(pivot_index);
        else
            push_down_msgbuf(pivot_index);
    }

    return true;
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
    nid_t child = pivots_[pivot_index].child_nid;
    assert(child != NID_NIL); 

    Node* node = tree_->get_node_by_nid(child);
    assert(node);
    
    MsgBuf* msgbuf = pivots_[pivot_index].msgbuf;
    for (MsgBuf::Iterator it = msgbuf->begin(); it != msgbuf->end(); it++) {
        // TODO: optimization
        node->write(*it);
    }
    msgbuf->release();
}

void Node::split_msgbuf(size_t pivot_index)
{
    MsgBuf* srcbuf = pivots_[pivot_index].msgbuf;
    MsgBuf* dstbuf = new MsgBuf(tree_->options_.comparator);    
    
    size_t half = srcbuf->msg_count() / 2;
    Slice key = (srcbuf->begin() + half)->key();

    MsgBuf::Iterator first = srcbuf->begin() + half;
    MsgBuf::Iterator last  = srcbuf->end();

    dstbuf->insert(dstbuf->begin(), first, last);
    srcbuf->resize(half);

    pivots_.insert(pivots_.begin() + pivot_index + 1, 
                   Pivot(NID_NIL, dstbuf, key.clone()));

    try_split_node();
}

void Node::try_split_node()
{
    if (pivots_.size() <= tree_->options_.max_node_child_number) return;

    size_t half = pivots_.size() / 2;
    Slice half_key = pivots_[half].left_most_key();

    Node* node = tree_->create_node(parent_nid_);
    
    Container::iterator first = pivots_.begin() + half;
    Container::iterator last  = pivots_.end();

    node->pivots_.insert(node->pivots_.begin(), first, last);
    pivots_.resize(half); 

    if (parent_nid_ == NID_NIL) {
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
        parent->add_pivot(half_key, self_nid_);
    }
}

void Node::add_pivot(Slice key, nid_t child)
{
    size_t pivot_index = find_pivot(key);
    MsgBuf* msgbuf = new MsgBuf(tree_->options_.comparator);

    pivots_.insert(pivots_.begin() + pivot_index + 1, 
                   Pivot(child, msgbuf, key.clone()));

    try_split_node();
}
