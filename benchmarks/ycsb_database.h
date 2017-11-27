#ifndef DBX1000_YCSB_DATABASE_H
#define DBX1000_YCSB_DATABASE_H

#include "database.h"

class YCSBDatabase : public Database {
public :
    void        initialize(uint32_t num_threads = INIT_PARALLELISM) override;
    txn_man *   get_txn_man(uint32_t thread_id) override;
    int         key_to_part(uint64_t key);
    INDEX * the_index;
    table_t * the_table;

protected:
    void        load_tables(uint32_t thread_id) override;
    void        load_main_table(uint32_t thread_id);

};


#endif //DBX1000_YCSB_DATABASE_H
