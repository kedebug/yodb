#include <gtest/gtest.h>
#include "tree/buffer_tree.h"

using namespace yodb;

TEST(BufferTree, bootstrap)
{
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.max_node_msg_count = 4;
    opts.max_node_child_number = 2;

    BufferTree* tree = new BufferTree("", opts);
    tree->init_tree();
    Node* n1 = tree->root_;

    n1->put("a", "1");
    n1->put("b", "1");
    n1->put("c", "1");
    n1->put("d", "1");

    EXPECT_EQ(n1->pivots_.size(), 1);
    EXPECT_EQ(n1->pivots_[0].msgbuf->msg_count(), 4);

    n1->put("e", "1");
    // split msgbuf
    EXPECT_EQ(n1->pivots_.size(), 2); 
    EXPECT_EQ(n1->pivots_[0].msgbuf->msg_count(), 2);
    EXPECT_EQ(n1->pivots_[1].msgbuf->msg_count(), 3);

    n1->put("f", "1");
    EXPECT_EQ(n1->pivots_[1].msgbuf->msg_count(), 4);
    n1->put("aa", "1");
    EXPECT_EQ(n1->pivots_[0].msgbuf->msg_count(), 3);

    n1->put("g", "1");
    // split msgbuf, add pivot, then split node
    Node* n2 = tree->root_;
    EXPECT_EQ(n2->pivots_.size(), 2);

    Node* n2_1 = tree->get_node_by_nid(n2->pivots_[0].child_nid);
    Node* n2_2 = tree->get_node_by_nid(n2->pivots_[1].child_nid);

    EXPECT_EQ(n2_1->pivots_.size(), 1);
    EXPECT_EQ(n2_2->pivots_.size(), 2);

    n2->put("bb", "1");
    EXPECT_EQ(n2->pivots_[0].msgbuf->msg_count(), 1);
    n2->put("a", "1");
    n2->put("aa", "1");
    n2->put("b", "1");
    n2->put("bb", "2");
    EXPECT_EQ(n2->pivots_[0].msgbuf->msg_count(), 4);
    n2->put("bc", "1");
    // n2's msgbuf should push down to n2_1
    EXPECT_EQ(n2_1->pivots_[0].msgbuf->msg_count(), 2);
    EXPECT_EQ(n2_1->pivots_[1].msgbuf->msg_count(), 3);
   
    n2->put("e", "1");
    n2->put("f", "1");
    n2->put("g", "1");
    n2->put("h", "1");
    n2->put("hh", "1");
    // n2's msgbuf should push down to n2_2
    // the tree has 3 levels NOW
    
    Node* n3 = tree->root_;
    EXPECT_EQ(n3->pivots_.size(), 2);
    EXPECT_EQ(n3->pivots_[0].child_nid, n2->self_nid_);

    delete opts.comparator;
    delete tree;
}
