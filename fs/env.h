#ifndef _YODB_ENV_H_
#define _YODB_ENV_H_

#include "fs/file.h"
#include <string>
#include <boost/noncopyable.hpp>

namespace yodb {

class Env : boost::noncopyable {
public:
    Env(const std::string& dirname)
        : dirname_(dirname)
    {
    }

    AIOFile* open_aio_file(const std::string& filename)
    {
        AIOFile* faio = new AIOFile(full_path(filename));

        if (faio && faio->open())
            return faio;
    
        delete faio;
        return NULL;
    }

    std::string full_path(const std::string& filename)
    {
        return dirname_ + "/" + filename;
    }
private:
    std::string dirname_;
};

} // namespace yodb

#endif // _YODB_ENV_H_
