// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_DATABASE_H_
#define TPCC_TPCC_DATABASE_H_

#include "database.h"
#include "txn.h"
#include "thread.h"
#include "tpcc_helper.h"
#include "tpcc.h"

class TPCCDatabase : public Database {
public:
		void initialize(uint32_t num_threads) override;
		txn_man *get_txn_man(uint32_t thread_id) override;

		table_t *t_warehouse;
		table_t *t_district;
		table_t *t_customer;
		table_t *t_history;
		table_t *t_new_order;
		table_t *t_order;
		table_t *t_order_line;
		table_t *t_item;
		table_t *t_stock;

		INDEX *i_item;
		INDEX *i_warehouse;
		INDEX *i_district;
		INDEX *i_customer_id;
		INDEX *i_customer_last;
		INDEX *i_stock;

protected:
		TPCCUtility * utility;
		void load_tables(uint32_t thread_id) override;

private:
		void load_items_table();
		void load_warehouse_table(uint32_t wid);
		void load_districts_table(uint64_t w_id);
		void load_stocks_table(uint64_t w_id);
		void load_customer_table(uint64_t d_id, uint64_t w_id);
		void load_order_table(uint64_t d_id, uint64_t w_id);
		void load_history_table(uint64_t c_id, uint64_t d_id, uint64_t w_id);
		void initialize_permutation(uint64_t *perm_c_id, uint64_t wid);
};

class TPCCTransactionManager : public txn_man {
public:
		void initialize(Database *database, uint32_t thread_id) override;
		RC run_txn(BaseQuery *query) override;

private:
		TPCCDatabase *db;
		RC run_payment(tpcc_payment_params *params);
		RC run_new_order(tpcc_new_order_params *m_query);
};


#endif // TPCC_TPCC_DATABASE_H_