#include "tpcc_workload.h"

TPCCWorkloadGenerator::TPCCWorkloadGenerator(const TPCCBenchmarkConfig &_config,
                                             uint64_t num_threads,
                                             uint64_t size_per_thread,
                                             const string &folder_path)
    : ParallelWorkloadGenerator<tpcc_params>(num_threads, size_per_thread,
                                             folder_path),
      config(_config), helper(_config.num_warehouses), _random(num_threads) {
  for (uint64_t i = 0; i < _config.num_warehouses; i++) {
    helper.random.seed(i, i + 1);
  }

  for (uint64_t i = 0; i < num_threads; i++) {
    _random.seed(i, 2 * i + 1);
  }
}

void TPCCWorkloadGenerator::per_thread_generate(uint64_t thread_id) {
  for (uint64_t i = 0; i < _num_queries_per_thread; i++) {
    double x = _random.nextDouble(thread_id);
    if (x < config.percent_payment) {
      _queries[thread_id][i].type = TPCC_PAYMENT_QUERY;
      gen_payment_request(
          thread_id, (tpcc_payment_params *)&(_queries[thread_id][i].params));
    } else {
      _queries[thread_id][i].type = TPCC_NEW_ORDER_QUERY;
      gen_new_order_request(
          thread_id, (tpcc_new_order_params *)&(_queries[thread_id][i].params));
    }
  }
}

void TPCCWorkloadGenerator::gen_payment_request(uint64_t thread_id,
                                                tpcc_payment_params *params) {
  if (FIRST_PART_LOCAL) {
    params->w_id = (thread_id % config.num_warehouses) + 1;
  } else {
    params->w_id = (_random.nextInt64(thread_id) % config.num_warehouses) + 1;
  }

  params->d_w_id = params->w_id;
  params->d_id = helper.generateRandom(1, config.districts_per_warehouse,
                                       params->w_id - 1);
  params->h_amount = helper.generateRandom(1, 5000, params->w_id - 1);

  auto x = (int)helper.generateRandom(1, 100, params->w_id - 1);
  if (x <= 85) {
    // home warehouse
    params->c_d_id = params->d_id;
    params->c_w_id = params->w_id;
  } else {
    // remote warehouse
    params->c_d_id = helper.generateRandom(1, config.districts_per_warehouse,
                                           params->w_id - 1);
    if (config.num_warehouses > 1) {
      // generate something other than params->w_id
      while ((params->c_w_id = helper.generateRandom(1, config.num_warehouses,
                                                     params->w_id - 1)) ==
             params->w_id) {
      }
    } else {
      params->c_w_id = params->w_id;
    }
  }

  auto y = (int)helper.generateRandom(1, 100, params->w_id - 1);
  if (y <= 60) {
    // by last name
    params->by_last_name = true;
    TPCCUtility::findLastNameForNum(
        helper.generateNonUniformRandom(255, 0, 999, params->w_id - 1),
        params->c_last);
  } else {
    // by customer id
    params->by_last_name = false;
    params->c_id = helper.generateNonUniformRandom(
        1023, 1, config.customers_per_district, params->w_id - 1);
  }
}

void TPCCWorkloadGenerator::gen_new_order_request(
    uint64_t thread_id, tpcc_new_order_params *params) {
  // choose a home warehouse
  if (FIRST_PART_LOCAL) {
    params->w_id = thread_id % config.num_warehouses + 1;
  } else {
    params->w_id = helper.generateRandom(1, config.num_warehouses,
                                         thread_id % config.num_warehouses);
  }

  params->d_id = helper.generateRandom(1, config.districts_per_warehouse,
                                       params->w_id - 1);
  params->c_id = helper.generateNonUniformRandom(
      1023, 1, config.customers_per_district, params->w_id - 1);
  params->rbk = (bool)helper.generateRandom(1, 100, params->w_id - 1);

  params->o_entry_d = 2013;
  params->ol_cnt = helper.generateRandom(5, 15, params->w_id - 1);

  params->remote = false;
  for (uint32_t oid = 0; oid < params->ol_cnt; oid++) {
    // choose a random item
    params->items[oid].ol_i_id = helper.generateNonUniformRandom(
        8191, 1, config.items_count, params->w_id - 1);

    // 1% of ol items go remote
    auto x = (uint32_t)helper.generateRandom(1, 100, params->w_id - 1);
    if (x > 1 || config.num_warehouses == 1) {
      params->items[oid].ol_supply_w_id = params->w_id;
    } else {
      while ((params->items[oid].ol_supply_w_id =
                  helper.generateRandom(1, config.num_warehouses,
                                        params->w_id - 1)) == params->w_id) {
      }
      params->remote = true;
      params->items[oid].ol_quantity =
          helper.generateRandom(1, 10, params->w_id - 1);
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

TPCCExecutor::TPCCExecutor(const TPCCBenchmarkConfig &config,
                           const string &folder_path, uint64_t num_threads)
    : BenchmarkExecutor<tpcc_params>(folder_path, num_threads), _db(config),
      _loader(folder_path, num_threads) {

  // Build database in parallel
  _db.initialize(FLAGS_load_parallelism);
  _db.load();

  // Load workload in parallel
  _loader.load();

  // Initialize each thread
  for (uint32_t i = 0; i < _num_threads; i++) {
    _threads[i].initialize(i, &_db, _loader.get_queries_list(i), true);
  }
}
