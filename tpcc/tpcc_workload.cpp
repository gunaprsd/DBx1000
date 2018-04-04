#include "tpcc_workload.h"
#include "scheduler.h"

TPCCWorkloadGenerator::TPCCWorkloadGenerator(const TPCCBenchmarkConfig &_config,
                                             uint64_t num_threads, uint64_t size_per_thread,
                                             const string &folder_path)
    : ParallelWorkloadGenerator<tpcc_params>(num_threads, size_per_thread, folder_path),
      config(_config), helper(_config.num_warehouses), _random(num_threads) {
    for (uint64_t i = 0; i < _config.num_warehouses; i++) {
        helper.random.seed(i, i + 1);
    }

    for (uint64_t i = 0; i < num_threads; i++) {
        _random.seed(i, 2 * i + 1);
    }

    num_orders = new uint64_t[num_threads];
    num_order_txns = new uint64_t[num_threads];
    for (uint64_t i = 0; i < num_threads; i++) {
        num_orders[i] = 0;
        num_order_txns[i] = 0;
    }
}

void TPCCWorkloadGenerator::per_thread_generate(uint64_t thread_id) {
    for (uint64_t i = 0; i < _num_queries_per_thread; i++) {
        double x = _random.nextDouble(thread_id);
        if (x < config.percent_payment) {
            _queries[thread_id][i].type = TPCC_PAYMENT_QUERY;
            gen_payment_request(thread_id, &_queries[thread_id][i]);
        } else {
            _queries[thread_id][i].type = TPCC_NEW_ORDER_QUERY;
            gen_new_order_request(thread_id, &_queries[thread_id][i]);
        }
    }
    double avg = ((double)num_orders[thread_id]) / ((double)num_order_txns[thread_id]);
    printf("Thread : %lu, Num-Order-Txns: %lu, Avg-Orders-Per-Txn: %lf\n", thread_id,
           num_order_txns[thread_id], avg);
}

void TPCCWorkloadGenerator::gen_payment_request(uint64_t thread_id, tpcc_query *query) {

    auto params = reinterpret_cast<tpcc_payment_params *>(&query->params);
    if (FIRST_PART_LOCAL) {
        params->w_id = (thread_id % config.num_warehouses) + 1;
    } else {
        params->w_id = (_random.nextInt64(thread_id) % config.num_warehouses) + 1;
    }

    params->d_w_id = params->w_id;
    params->d_id = helper.generateRandom(1, config.districts_per_warehouse, params->w_id - 1);

    params->h_amount = helper.generateRandom(1, 5000, params->w_id - 1);

    auto x = (int)helper.generateRandom(1, 100, params->w_id - 1);
    if (x > FLAGS_tpcc_remote_payment_percent) {
        // home warehouse
        params->c_d_id = params->d_id;
        params->c_w_id = params->w_id;
    } else {
        // remote warehouse
        params->c_d_id = helper.generateRandom(1, config.districts_per_warehouse, params->w_id - 1);
        if (config.num_warehouses > 1) {
            // generate something other than params->w_id
            while ((params->c_w_id = helper.generateRandom(1, config.num_warehouses,
                                                           params->w_id - 1)) == params->w_id) {
            }
        } else {
            params->c_w_id = params->w_id;
        }
    }

    auto y = (int)helper.generateRandom(1, 100, params->w_id - 1);
    if (y <= 100 * FLAGS_tpcc_by_last_name_percent) {
        // by last name
        params->by_last_name = true;
        TPCCUtility::findLastNameForNum(
            helper.generateNonUniformRandom(255, 0, 999, params->w_id - 1), params->c_last);
    } else {
        // by customer id
        params->by_last_name = false;
        params->c_id = helper.generateNonUniformRandom(1023, 1, config.customers_per_district,
                                                       params->w_id - 1);
    }
}

void TPCCWorkloadGenerator::gen_new_order_request(uint64_t thread_id, tpcc_query *query) {
    auto params = reinterpret_cast<tpcc_new_order_params *>(&query->params);
    // choose a home warehouse
    if (FIRST_PART_LOCAL) {
        params->w_id = thread_id % config.num_warehouses + 1;
    } else {
        params->w_id =
            helper.generateRandom(1, config.num_warehouses, thread_id % config.num_warehouses);
    }

    params->d_id = helper.generateRandom(1, config.districts_per_warehouse, params->w_id - 1);


    params->c_id =
        helper.generateNonUniformRandom(1023, 1, config.customers_per_district, params->w_id - 1);

    params->rbk = (bool)helper.generateRandom(1, 100, params->w_id - 1);

    params->o_entry_d = 2013;
    if (TPCC_NUM_ORDERS_RANDOM) {
        params->ol_cnt = helper.generateRandom(6, 10, params->w_id - 1);
    } else {
        params->ol_cnt = TPCC_MAX_NUM_ORDERS;
    }

    params->remote = false;
    // Generate
    for (uint32_t oid = 0; oid < params->ol_cnt; oid++) {
        // choose a unique random item
        bool unique;
        do {
            //      params->items[oid].ol_i_id = helper.generateNonUniformRandom(
            //    8191, 1, config.items_count, params->w_id - 1);
            params->items[oid].ol_i_id =
                helper.generateRandom(1, config.items_count, params->w_id - 1);
            unique = true;
            for (uint32_t i = 0; i < oid; i++) {
                if (params->items[i].ol_i_id == params->items[oid].ol_i_id) {
                    unique = false;
                    break;
                }
            }
        } while (!unique);

        // 1% of ol items go remote
        auto x = (uint32_t)helper.generateRandom(1, 100, params->w_id - 1);
        if (x > 1 || config.num_warehouses == 1) {
            params->items[oid].ol_supply_w_id = params->w_id;
        } else {
            while ((params->items[oid].ol_supply_w_id = helper.generateRandom(
                        1, config.num_warehouses, params->w_id - 1)) == params->w_id) {
            }
            params->remote = true;
            params->items[oid].ol_quantity = helper.generateRandom(1, 10, params->w_id - 1);
        }
    }

    for (uint32_t i = 0; i < params->ol_cnt; i++) {
        for (uint32_t j = 0; j < i; j++) {
            assert(params->items[(int)i].ol_i_id != params->items[(int)j].ol_i_id);
        }
    }

    num_orders[thread_id] += params->ol_cnt;
    num_order_txns[thread_id] += 1;
}

TPCCExecutor::TPCCExecutor(const TPCCBenchmarkConfig &config, const string &folder_path,
                           uint64_t num_threads)
    : _db(config),
      _loader(folder_path, num_threads) {

    // Build database in parallel
    _db.initialize(FLAGS_load_parallelism);
    _db.load();

    // Load workload in parallel
    _loader.load();

    // Initialize each thread
    _scheduler = new OnlineScheduler<tpcc_params>(num_threads, &_db);
}

void TPCCExecutor::execute() {
    _scheduler->schedule(&_loader);
}

void TPCCExecutor::release() {
    _loader.release();
}
