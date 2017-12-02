#include "graph_partitioner.h"
#include "tpcc.h"


BaseQueryList * TPCCWorkloadGenerator::get_queries_list(uint32_t thread_id) {
    auto queryList = new QueryList<tpcc_params>();
    queryList->initialize(_queries[thread_id], _num_params_per_thread);
    return queryList;
}

BaseQueryMatrix *TPCCWorkloadGenerator::get_queries_matrix() {
    auto matrix = new QueryMatrix<tpcc_params>();
    matrix->initialize(_queries, _num_threads, _num_params_per_thread);
    return matrix;
}

void TPCCWorkloadGenerator::per_thread_generate(uint32_t thread_id) {
    for(uint32_t i = 0; i < _num_params_per_thread; i++) {
        double x = (double) (rand() % 100) / 100.0;
        if(x < g_perc_payment) {
            _queries[thread_id][i].type = TPCC_PAYMENT_QUERY;
            gen_payment_request(thread_id, (tpcc_payment_params *)&(_queries[thread_id][i].params));
        } else {
            _queries[thread_id][i].type = TPCC_NEW_ORDER_QUERY;
            gen_new_order_request(thread_id, (tpcc_new_order_params *)&(_queries[thread_id][i].params));
        }
    }
}

void TPCCWorkloadGenerator::per_thread_write_to_file(uint32_t thread_id, FILE *file) {
    tpcc_query * thread_queries = _queries[thread_id];
    fwrite(thread_queries, sizeof(tpcc_query), _num_params_per_thread, file);
}

void TPCCWorkloadGenerator::gen_payment_request(uint64_t thread_id, tpcc_payment_params * params) {
    if(FIRST_PART_LOCAL) {
        params->w_id = thread_id % g_num_wh + 1;
    } else {
        params->w_id = URand(1, g_num_wh, thread_id % g_num_wh);
    }

    params->d_w_id = params->w_id;
    params->d_id = URand(1, DIST_PER_WARE, params->w_id - 1);
    params->h_amount = URand(1, 5000, params->w_id - 1);

    auto x = (int) URand(1, 100, params->w_id - 1);
    if(x <= 85) {
        // home warehouse
        params->c_d_id = params->d_id;
        params->c_w_id = params->w_id;
    } else {
        // remote warehouse
        params->c_d_id = URand(1, DIST_PER_WARE, params->w_id - 1);
        if(g_num_wh > 1) {
            //generate something other than params->w_id
            while((params->c_w_id = URand(1, g_num_wh, params->w_id - 1)) == params->w_id) {}
        } else {
            params->c_w_id = params->w_id;
        }
    }

    auto y = (int) URand(1, 100, params->w_id - 1);
    if(y <= 60) {
        // by last name
        params->by_last_name = true;
        Lastname(NURand(255,0,999, params->w_id - 1), params->c_last);
    } else {
        // by customer id
        params->by_last_name = false;
        params->c_id = NURand(1023, 1, g_cust_per_dist, params->w_id-1);
    }
}

void TPCCWorkloadGenerator::gen_new_order_request(uint64_t thd_id, tpcc_new_order_params * params) {
    //choose a home warehouse
    if (FIRST_PART_LOCAL) {
        params->w_id = thd_id % g_num_wh + 1;
    } else {
        params->w_id = URand(1, g_num_wh, thd_id % g_num_wh);
    }

    params->d_id    = URand(1, DIST_PER_WARE, params->w_id - 1);
    params->c_id    = NURand(1023, 1, g_cust_per_dist, params->w_id - 1);
    params->rbk     = (bool) URand(1, 100, params->w_id - 1);

    params->o_entry_d   = 2013;
    params->ol_cnt      = URand(5, 15, params->w_id - 1);

    params->remote = false;
    for (uint32_t oid = 0; oid < params->ol_cnt; oid ++) {
        //choose a random item
        params->items[oid].ol_i_id = NURand(8191, 1, g_max_items, params->w_id - 1);

        //1% of ol items go remote
        auto x = (uint32_t) URand(1, 100, params->w_id - 1);
        if (x > 1 || g_num_wh == 1) {
            params->items[oid].ol_supply_w_id = params->w_id;
        } else {
            while ((params->items[oid].ol_supply_w_id = URand(1, g_num_wh, params->w_id - 1)) == params->w_id) {}
            params->remote = true;
            params->items[oid].ol_quantity = URand(1, 10, params->w_id - 1);
        }

        // Remove duplicate items
        for (uint32_t i = 0; i < params->ol_cnt; i++) {
            for (uint32_t j = 0; j < i; j++) {
                if (params->items[i].ol_i_id == params->items[j].ol_i_id) {
                    for (uint32_t k = i; k < params->ol_cnt - 1; k++) {
                        params->items[k] = params->items[k + 1];
                    }
                    params->ol_cnt--;
                    i--;
                }
            }
        }

        for (uint32_t i = 0; i < params->ol_cnt; i++) {
            for (uint32_t j = 0; j < i; j++) {
                assert(params->items[(int)i].ol_i_id != params->items[(int)j].ol_i_id);
            }
        }
    }
}

void TPCCWorkloadGenerator::initialize(uint32_t num_threads,
                                       uint64_t num_params_per_thread,
                                       const char *base_file_name) {
    ParallelWorkloadGenerator::initialize(num_threads, num_params_per_thread, base_file_name);
    _queries = new tpcc_query * [_num_threads];
    for(uint32_t i = 0; i < _num_threads; i++) {
        _queries[i] = new tpcc_query[_num_params_per_thread];
    }

    if(tpcc_buffer == nullptr) {
        tpcc_buffer = new drand48_data * [_num_threads];
        for(uint32_t i = 0; i < _num_threads; i++) {
            tpcc_buffer[i] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
            srand48_r((i+1), tpcc_buffer[i]);
        }
    }
}



BaseQueryList * TPCCWorkloadLoader::get_queries_list(uint32_t thread_id) {
    auto queryList = new QueryList<tpcc_params>();
    queryList->initialize(_queries[thread_id], _array_sizes[thread_id]);
    return queryList;
}

void TPCCWorkloadLoader::per_thread_load(uint32_t thread_id, FILE *file) {
    fseek(file, 0, SEEK_END);
    size_t bytes_to_read = ftell(file);
    fseek(file, 0, SEEK_SET);

    _array_sizes[thread_id] = bytes_to_read / sizeof(tpcc_query);
    _queries[thread_id] 		= (tpcc_query *) _mm_malloc(bytes_to_read, 64);

    size_t bytes_read = fread(_queries[thread_id], sizeof(tpcc_query), _array_sizes[thread_id], file);
    assert(bytes_read == bytes_to_read);
}

void TPCCWorkloadLoader::initialize(uint32_t num_threads, char *base_file_name) {
    ParallelWorkloadLoader::initialize(num_threads, base_file_name);
    _queries = new tpcc_query * [_num_threads];
    _array_sizes = new uint32_t[_num_threads];
}



BaseQueryList *TPCCWorkloadPartitioner::get_queries_list(uint32_t thread_id) {
    auto queryList = new QueryList<tpcc_params>();
    queryList->initialize(_partitioned_queries[thread_id], _tmp_queries[thread_id].size());
    return queryList;
}

void TPCCWorkloadPartitioner::initialize(BaseQueryMatrix * queries,
                                         uint64_t max_cluster_graph_size,
                                         uint32_t parallelism) {
    ParallelWorkloadPartitioner::initialize(queries, max_cluster_graph_size, parallelism);
    _partitioned_queries = nullptr;
}

void TPCCWorkloadPartitioner::partition() {
    ParallelWorkloadPartitioner::partition();

    uint64_t start_time, end_time;
    start_time = get_server_clock();
    _partitioned_queries = new tpcc_query * [_num_arrays];
    for(uint32_t i = 0; i < _num_arrays; i++) {
        _partitioned_queries[i] = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * _tmp_queries[i].size(), 64);
        uint32_t offset = 0;
        for(auto iter = _tmp_queries[i].begin(); iter != _tmp_queries[i].end(); iter++) {
            memcpy(& _partitioned_queries[i][offset], * iter, sizeof(tpcc_query));
            offset++;
        }
    }
    end_time = get_server_clock();
    shuffle_duration += DURATION(end_time, start_time);
}

void TPCCWorkloadPartitioner::write_workload_file(uint32_t thread_id, FILE *file) {
    tpcc_query * thread_queries = _partitioned_queries[thread_id];
    uint32_t size = _tmp_array_sizes[thread_id];
    fwrite(thread_queries, sizeof(tpcc_query), size, file);
}

void TPCCExecutor::initialize(uint32_t num_threads) {
    BenchmarkExecutor::initialize(num_threads);

    //Build database in parallel
    _db = new TPCCDatabase();
    _db->initialize(INIT_PARALLELISM);
    _db->load();

    //Generate workload in parallel
    _generator = new TPCCWorkloadGenerator();
    _generator->initialize(_num_threads, MAX_TXN_PER_PART, nullptr);
    _generator->generate();

    _partitioner = new TPCCWorkloadPartitioner();
    _partitioner->initialize(_generator->get_queries_matrix(), MAX_NODES_FOR_CLUSTERING, INIT_PARALLELISM);
    _partitioner->partition();

    //Initialize each thread
    for(uint32_t i = 0; i < _num_threads; i++) {
        _threads[i].initialize(i, _db, _partitioner->get_queries_list(i), true);
    }
}