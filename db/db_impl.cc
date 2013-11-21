#include "db/db_impl.h"

using namespace yodb;

DBImpl::~DBImpl()
{
    delete tree_;
}

bool DBImpl::init()
{
    if (opts_.comparator == NULL) {
        return false;
    }
    tree_ = new BufferTree(name_, opts_); 
    if (tree_ == NULL)
        return false;
    return tree_->init_tree();
}

bool DBImpl::put(const Slice& key, const Slice& value)
{
    return tree_->put(key, value);
}

bool DBImpl::del(const Slice& key)
{
    return tree_->del(key);
}

bool DBImpl::get(const Slice& key, Slice& value)
{
    return tree_->get(key, value);
}
