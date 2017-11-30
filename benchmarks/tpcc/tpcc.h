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
    RC run_payment(tpcc_payment_params * params);
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
    table_t *		t_order_line;
    table_t *		t_item;
    table_t *		t_stock;

    INDEX * 	i_item;
    INDEX * 	i_warehouse;
    INDEX * 	i_district;
    INDEX * 	i_customer_id;
    INDEX * 	i_customer_last;
    INDEX * 	i_stock;

protected:
    void                load_tables(uint32_t thread_id) override;
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
    void 	        gen_payment_request(uint64_t thread_id, tpcc_payment_params * params);
    void            gen_new_order_request(uint64_t thd_id, tpcc_new_order_params * params);

    tpcc_query * * 		_queries;

    friend class TPCCWorkloadPartitioner;
};

class TPCCWorkloadPartitioner : public WorkloadPartitioner {
public:
    void initialize(uint32_t num_threads,
                    uint64_t num_params_per_thread,
                    uint64_t num_params_pgpt,
                    ParallelWorkloadGenerator * generator) override;
    BaseQueryList * get_queries_list(uint32_t thread_id) override;
    void partition() override;
 protected:
    void partition_workload_part(uint32_t iteration, uint64_t num_records) override;
    tpcc_query * * _orig_queries;
    tpcc_query * * _partitioned_queries;
private:

    int compute_weight(tpcc_query * q1, tpcc_query * q2, DataInfo * data) {
        if(q1->type == TPCC_PAYMENT_QUERY && q2->type == TPCC_PAYMENT_QUERY) {
            return compute_weight((tpcc_payment_params *) & q1->params, (tpcc_payment_params *) & q2->params);
        } else if(q1->type == TPCC_NEW_ORDER_QUERY && q2->type == TPCC_NEW_ORDER_QUERY) {
            return compute_weight((tpcc_new_order_params *) & q1->params, (tpcc_new_order_params *) & q2->params);
        } else if(q1->type == TPCC_PAYMENT_QUERY && q2->type == TPCC_NEW_ORDER_QUERY) {
            return compute_weight((tpcc_payment_params *) & q1->params, (tpcc_new_order_params *) & q2->params);
        } else {
            return compute_weight((tpcc_payment_params *) & q2->params, (tpcc_new_order_params *) & q1->params);
        }
    }

    int compute_weight(tpcc_payment_params * q1, tpcc_payment_params * q2) {
        //Don't know if c_last and c_id match - so ignore!
        if(q1->w_id == q2->w_id) {
            if(q1->d_id == q2->d_id) {
                return 10;
            } else {
                return 9;
            }
        } else {
            return -1;
        }
    }

    int compute_weight(tpcc_new_order_params * q1, tpcc_new_order_params * q2) {
        int weight = -1;
        if(q1->w_id == q2->w_id) {
            if(q1->d_id == q2->d_id) {
                weight += 20;
            } else {
                weight += 19;
            }
        }

        for(uint32_t i = 0; i < q1->ol_cnt; i++) {
            for(uint32_t j = 0; j < q2->ol_cnt; j++) {
                if(q1->items[i].ol_supply_w_id == q2->items[i].ol_supply_w_id && q1->items[i].ol_i_id == q2->items[i].ol_i_id) {
                        weight += 1;
                }
            }
        }

        return weight;
    }

    int compute_weight(tpcc_payment_params * q1, tpcc_new_order_params * q2) {
        if(q1->w_id == q2->w_id) {
            if(q1->d_id == q2->d_id) {
                if(q1->c_id == q2->c_id) {
                    return 10;
                } else {
                    return 9;
                }
            } else {
                return 8;
            }
        } else {
            return -1;
        }
    }
};

#endif //DBX1000_TPCC_H

