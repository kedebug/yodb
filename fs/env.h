#ifndef _YODB_ENV_H_
#define _YODB_ENV_H_

#include "fs/file.h"
#include <string>
#include <sys/stat.h>
#include <boost/noncopyable.hpp>

namespace yodb {

class Env : boost::noncopyable {
public:
    Env(const std::string& dirname)
        : dirname_(dirname)
    {
    }

    bool file_exists(const std::string& filename) 
    {
        struct stat st;
        memset(&st, 0, sizeof(st));

        if (stat(full_path(filename).c_str(), &st) == -1) {
            LOG_ERROR << "stat file: " << filename << ", error: " << strerror(errno);
            return false;
        }

        return S_ISREG(st.st_mode);
    }

    size_t file_length(const std::string& filename)
    {
        struct stat st;
        memset(&st, 0, sizeof(st));

        if (stat(full_path(filename).c_str(), &st) == -1) {
            LOG_ERROR << "stat file: " << filename << " error" << strerror(errno);
            return 0;
        }

        return (size_t)(st.st_size);
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
