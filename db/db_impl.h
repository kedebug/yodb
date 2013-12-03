#ifndef _YODB_DB_IMPL_H_
#define _YODB_DB_IMPL_H_

#include "db/options.h"
#include "tree/buffer_tree.h"

namespace yodb {

class DBImpl {
public:
    DBImpl(const std::string& name, const Options& opts)
        : name_(name), opts_(opts), file_(NULL),
          table_(NULL), cache_(NULL), tree_(NULL)
    {
    }

    ~DBImpl();
    
    bool init();

    bool put(const Slice& key, const Slice& value);
    bool del(const Slice& key);
    bool get(const Slice& key, Slice& value);

private:
    std::string name_;
    Options opts_;

    AIOFile* file_;
    Table* table_;
    Cache* cache_;
    BufferTree* tree_;
};

} // namespace yodb

#endif // _YODB_DB_IMPL_H_
