#include "db/db_impl.h"

using namespace yodb;

DBImpl::~DBImpl()
{
    delete tree_;
    delete cache_;
    delete table_;
    delete file_;
}

bool DBImpl::init()
{
    if (opts_.comparator == NULL) {
        LOG_ERROR << "Comparator must be set";
        return false;
    }
    
    Env* env = opts_.env;
    if (env == NULL) {
        LOG_ERROR << "File environment must be set";
        return false;
    }

    size_t size = 0;
    bool create = true;

    if (env->file_exists(name_)) {
        size = env->file_length(name_);
        if (size > 0)
            create = false;
    }

    file_ = env->open_aio_file(name_);

    table_ = new Table(file_, size);
    if (!table_->init(create)) {
        LOG_ERROR << "init table error";
        return false;
    }

    cache_ = new Cache(opts_);
    if (!cache_->init()) {
        LOG_ERROR << "init cache error";
        return false;
    }

    tree_ = new BufferTree(name_, opts_, cache_, table_); 
    if (!tree_->init()) {
        LOG_ERROR << "init buffer tree error";
        return false;
    }

    return true;
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
