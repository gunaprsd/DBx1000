#ifndef DBX1000_DATABASE_H
#define DBX1000_DATABASE_H

#include "global.h"
#include "config.h"

class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class lock_man;
class txn_man;
class index_base;
class Timestamp;
class Mvcc;


class Database {
public:
    virtual void        initialize(uint32_t num_threads = INIT_PARALLELISM);
    void                load();
    virtual txn_man *   get_txn_man(uint32_t thread_id) = 0;
protected:
    RC                  initialize_schema(string schema_file);
    static  void *       run_helper(void * ptr);
    virtual void        load_tables(uint32_t thread_id) = 0;

    void                index_insert(string index_name, uint64_t key, row_t * row);
    void                index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id = -1);

    std::map<string, table_t *> tables;
    std::map<string, INDEX *> indexes;
    uint32_t _num_threads;
};

#endif //DBX1000_DATABASE_H
