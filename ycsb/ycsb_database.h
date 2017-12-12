// Copyright [2017] <Guna Prasaad>

#ifndef YCSB_YCSB_DATABASE_H_
#define YCSB_YCSB_DATABASE_H_

#include "database.h"
#include "txn.h"
#include "ycsb.h"

class YCSBDatabase : public Database {
public:
		void initialize(uint32_t num_threads) override;
		txn_man *get_txn_man(uint32_t thread_id) override;
		int key_to_part(uint64_t key);
protected:
		void load_tables(uint32_t thread_id) override;
		void load_main_table(uint32_t thread_id);
public:
		INDEX *the_index;
		table_t *the_table;
};

class YCSBTransactionManager : public txn_man {
public:
		void initialize(Database *database, uint32_t thread_id) override;
		RC run_txn(BaseQuery *query) override;
		uint64_t row_cnt;
		YCSBDatabase * db;
};

#endif //YCSB_YCSB_DATABASE_H_
