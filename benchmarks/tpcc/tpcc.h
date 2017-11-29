#ifndef DBX1000_TPCC_H
#define DBX1000_TPCC_H

#include "global.h"
#include "helper.h"
#include "table.h"
#include "database.h"
#include "txn.h"
#include "query.h"
#include "workload.h"
#include "tpcc_helper.h"
#include "tpcc_const.h"

struct ol_item {
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
};

struct tpcc_payment_params {
    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
    uint64_t d_w_id;
    uint64_t c_w_id;
    uint64_t c_d_id;
    char c_last[LASTNAME_LEN];
    double h_amount;
    bool by_last_name;
};

struct tpcc_new_order_params {
    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
    ol_item items[MAX_NUM_ORDER_LINE];
    bool rbk;
    bool remote;
    uint64_t ol_cnt;
    uint64_t o_entry_d;
};

union tpcc_params {
    tpcc_new_order_params   new_order_params;
    tpcc_payment_params     payment_params;
};

typedef Query<tpcc_params> tpcc_query;

class TPCCDatabase;

class TPCCTransactionManager : public txn_man {
public:
    void initialize(Database * database, uint32_t thread_id) override;
    RC run_txn(BaseQuery * query) override;
private:

    TPCCDatabase * db;
    RC run_payment(tpcc_payment_params * m_query);
    RC run_new_order(tpcc_new_order_params * m_query);
};

class TPCCDatabase : public Database {
public:
    void        initialize(uint32_t num_threads) override;
    txn_man *   get_txn_man(uint32_t thread_id) override;

    table_t * 		t_warehouse;
    table_t * 		t_district;
    table_t * 		t_customer;
    table_t *		t_history;
    table_t *		t_new_order;
    table_t *		t_order;
    table_t *		t_orderline;
    table_t *		t_item;
    table_t *		t_stock;

    INDEX * 	i_item;
    INDEX * 	i_warehouse;
    INDEX * 	i_district;
    INDEX * 	i_customer_id;
    INDEX * 	i_customer_last;
    INDEX * 	i_stock;
protected:
    void        load_tables(uint32_t thread_id) override;
    drand48_data * * rand_buffer;
private:
    void load_items_table();
    void load_warehouse_table   (uint32_t wid);
    void load_districts_table   (uint64_t w_id);
    void load_stocks_table      (uint64_t w_id);
    void load_customer_table    (uint64_t d_id, uint64_t w_id);
    void load_order_table       (uint64_t d_id, uint64_t w_id);
    void load_history_table     (uint64_t c_id, uint64_t d_id, uint64_t w_id);

    void initialize_permutation(uint64_t * perm_c_id, uint64_t wid);
};

class TPCCWorkloadGenerator : public ParallelWorkloadGenerator {
public:
    void initialize(uint32_t num_threads,
                    uint64_t num_params_per_thread,
                    const char * base_file_name) override;

    BaseQueryList * get_queries_list(uint32_t thread_id) override;
protected:
    void            per_thread_generate(uint32_t thread_id) override;
    void            per_thread_write_to_file(uint32_t thread_id, FILE * file) override;
    void 			gen_payment_request(uint64_t thd_id, tpcc_query * query);
    void            gen_new_order_request(uint64_t thd_id, tpcc_query * query);

    tpcc_query * * 		_queries;
};

class TPCCWorkloadPartitioner : public WorkloadPartitioner {
protected:
    void partition_workload_part(uint32_t iteration, uint64_t num_records) override;
private:
    uint64_t hash(uint64_t key) { return key; }
    double compute_weight(tpcc_query * q1, tpcc_query * q2, DataInfo * data) {
        return 0.0;
    }
};

#endif //DBX1000_TPCC_H

