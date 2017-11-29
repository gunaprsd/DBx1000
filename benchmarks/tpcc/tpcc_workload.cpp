#include "tpcc.h"

BaseQueryList * TPCCWorkloadGenerator::get_queries_list(uint32_t thread_id) {
    auto queryList = new QueryList<tpcc_params>();
    queryList->initialize(_queries[thread_id], _num_params_per_thread);
    return queryList;
}

void TPCCWorkloadGenerator::per_thread_generate(uint32_t thread_id) {
    for(uint32_t i = 0; i < _num_params_per_thread; i++) {
        double x = (double) (rand() % 100) / 100.0;
        if(x < g_perc_payment) {
            _queries[thread_id][i].type = TPCC_PAYMENT_QUERY;
            gen_payment_request(thread_id, & _queries[thread_id][i]);
        } else {
            _queries[thread_id][i].type = TPCC_NEW_ORDER_QUERY;
            gen_new_order_request(thread_id, & _queries[thread_id][i]);
        }
    }
}

void TPCCWorkloadGenerator::per_thread_write_to_file(uint32_t thread_id, FILE *file) {
    tpcc_query * thread_queries = _queries[thread_id];
    fwrite(thread_queries, sizeof(tpcc_query), _num_params_per_thread, file);
}

void TPCCWorkloadGenerator::gen_payment_request(uint64_t thd_id, tpcc_query *query) {
    tpcc_params * tpcc_params = & query->params;
    auto params = (tpcc_payment_params *) tpcc_params;

    if(FIRST_PART_LOCAL) {
        params->w_id = thd_id % g_num_wh + 1;
    } else {
        params->w_id = URand(1, g_num_wh, thd_id % g_num_wh);
    }

    params->d_w_id = params->w_id;
    params->d_id = URand(1, DIST_PER_WARE, params->w_id - 1);
    params->h_amount = URand(1, 5000, params->w_id - 1);
    int x = (int) URand(1, 100, params->w_id - 1);
    int y = (int) URand(1, 100, params->w_id - 1);


    if(x <= 85) {
        // home warehouse
        params->c_d_id = params->d_id;
        params->c_w_id = params->w_id;
    } else {
        // remote warehouse
        params->c_d_id = URand(1, DIST_PER_WARE, params->w_id - 1);
        if(g_num_wh > 1) {
            while((params->c_w_id = URand(1, g_num_wh, params->w_id - 1)) == params->w_id) {}
        } else {
            params->c_w_id = params->w_id;
        }
    }

    if(y <= 60) {
        // by last name
        params->by_last_name = true;
        Lastname(NURand(255,0,999, params->w_id - 1), params->c_last);
    } else {
        // by cust id
        params->by_last_name = false;
        params->c_id = NURand(1023, 1, g_cust_per_dist, params->w_id-1);
    }
}

void TPCCWorkloadGenerator::gen_new_order_request(uint64_t thd_id, tpcc_query *query) {
    tpcc_params * tpcc_params = & query->params;
    auto params = (tpcc_new_order_params *) tpcc_params;

    if (FIRST_PART_LOCAL) {
        params->w_id = thd_id % g_num_wh + 1;
    } else {
        params->w_id = URand(1, g_num_wh, thd_id % g_num_wh);
    }

    params->d_id = URand(1, DIST_PER_WARE, params->w_id - 1);
    params->c_id = NURand(1023, 1, g_cust_per_dist, params->w_id - 1);

    params->rbk = (bool) URand(1, 100, params->w_id - 1);
    params->ol_cnt = URand(5, 15, params->w_id - 1);
    params->o_entry_d = 2013;

    params->remote = false;
    for (UInt32 oid = 0; oid < params->ol_cnt; oid ++) {
        params->items[oid].ol_i_id = NURand(8191, 1, g_max_items, params->w_id - 1);
        UInt32 x = (UInt32) URand(1, 100, params->w_id - 1);

        if (x > 1 || g_num_wh == 1) {
            params->items[oid].ol_supply_w_id = params->w_id;
        } else {
            while ((params->items[oid].ol_supply_w_id = URand(1, g_num_wh, params->w_id - 1)) == params->w_id) {}
            params->remote = true;
            params->items[oid].ol_quantity = URand(1, 10, params->w_id - 1);
        }

        // Remove duplicate items
        for (UInt32 i = 0; i < params->ol_cnt; i++) {
            for (UInt32 j = 0; j < i; j++) {
                if (params->items[i].ol_i_id == params->items[j].ol_i_id) {
                    for (UInt32 k = i; k < params->ol_cnt - 1; k++) {
                        params->items[k] = params->items[k + 1];
                    }
                    params->ol_cnt--;
                    i--;
                }
            }
        }

        for (UInt32 i = 0; i < params->ol_cnt; i++) {
            for (UInt32 j = 0; j < i; j++) {
                assert(params->items[(int)i].ol_i_id != params->items[(int)j].ol_i_id);
            }
        }
    }
}

void TPCCWorkloadGenerator::initialize(uint32_t num_threads, uint64_t num_params_per_thread, const char *base_file_name) {
    ParallelWorkloadGenerator::initialize(num_threads, num_params_per_thread, base_file_name);
    _queries = new tpcc_query * [_num_threads];
    for(uint32_t i = 0; i < _num_threads; i++) {
        _queries[i] = new tpcc_query[_num_params_per_thread];
    }
}

void TPCCWorkloadPartitioner::partition_workload_part(uint32_t iteration, uint64_t num_records) {

}
