yodb [![Total views](https://sourcegraph.com/api/repos/github.com/kedebug/yodb/counters/views.png)](https://sourcegraph.com/github.com/kedebug/yodb)
====

A lightweight and efficient key-value storage engine based on the buffer tree.

## Purpose
There have already been several KV databases like [tokudb](https://github.com/Tokutek/ft-index) and  [cascadb](https://github.com/weicao/cascadb), which are using buffer tree as the underlying data structure to optimize written operation. But their code seems to be a little complex and hard for the __beginners__ to understand the core idea of how the tree really works in multi-threaded environment.

So I write this storage engine which named __yodb__ (__yo__ just helps for funny pronunciation), try to meet the need of those guys who want to have a quick introspect of this beautiful algorithm. Yodb has an excellent performance that can handle millions of read/written requests at a time with only 6K source lines of code, and also of course has a detailed notation.

## Performance
#### Setup
We use a database with a million entries. Each entry has a 16 byte key, and a 100 byte value.
```
yodb:       version 0.1
Date:       Tue Dec 17 15:00:09 2013
CPU:        4 * Intel(R) Core(TM)2 Quad CPU    Q8300  @ 2.50GHz
CPUCache:   2048 KB
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    1000000
RawSize:    110.6 MB (estimated)
FileSize:   110.6 MB (estimated, compression disabled)
```
#### Write performance
```
fillseq      :       4.989 micros/op;   22.2 MB/s     
fillrandom   :       5.223 micros/op;   21.2 MB/s 
```
Each "op" above corresponds to a read/write of a single key/value pair. I.e., a random write benchmark goes at approximately 200,000 writes per second.

#### Read performance
```
readseq      :       2.653 micros/op;   41.7 MB/s  
readrandom   :       7.804 micros/op;    6.4 MB/s  
readhot      :       2.662 micros/op;   41.6 MB/s  
```


## Usage
#### Include to your header
```cpp
#include <yodb/db.h>

using namespace yodb;

Options opts;
opts.comparator = new BytewiseComparator();
opts.env = new Env("/your/database/path");

DB* db = new DB("your_db_name", opts);
if (!db->init()) {
    fprintf(stderr, "error initialize database\n");
}
```
#### Write
```cpp
if (!db->put("Shanghai", "Minhang part")) {
    fprintf(stderr, "insert error\n");
}
```
#### Read
```cpp
Slice value;
if (!db->get("Guangzhou", value)) {
    fprintf(stderr, "read error\n");
}
```
#### Delete
```cpp
if (!db->del("Beijing")) {
    fprintf(stderr, "delete error\n");
}
```
#### Exit
```cpp
delete db;
delete opts.comparator;
delete opts.env;
```

## Further work
- Make lock independent with the tree.
- Add bloom filter to accelerate read operation.
- ___Needs your help___
