// Copyright[2017] <Guna Prasaad>

#include "ycsb_workload.h"
#include <algorithm>

YCSBWorkloadGenerator::YCSBWorkloadGenerator(const YCSBBenchmarkConfig &_config,
                                             uint64_t num_threads,
                                             uint64_t size_per_thread,
                                             const string &folder_path)
    : ParallelWorkloadGenerator<ycsb_params>(num_threads, size_per_thread,
                                             folder_path),
      config(_config), zipfian(_config.table_size / _config.num_partitions,
                                _config.num_partitions, _config.zipfian_theta),
      _random(num_threads) {
  for (uint64_t i = 0; i < config.num_partitions; i++) {
    zipfian.seed(i, i + 1);
  }
  for (uint64_t i = 0; i < _num_threads; i++) {
    _random.seed(i, 3 * i + 1);
  }
}

void YCSBWorkloadGenerator::per_thread_generate(uint64_t thd_id) {
  for (uint64_t i = 0; i < _num_queries_per_thread; i++) {
    if (_random.nextDouble(thd_id) < config.multi_part_txns_percent) {
      gen_multi_partition_requests(thd_id, &(_queries[thd_id][i]));
    } else {
      gen_single_partition_requests(thd_id, &(_queries[thd_id][i]));
    }
  }
}

/*
 * Single Partition Transactions:
 * In this type of transactions, all requests pertain to a single partition.
 * We first choose a partition at random. Each partition has a zipfian access
 * distribution, which is used to generate the requests for that transaction.
 */
void YCSBWorkloadGenerator::gen_single_partition_requests(uint64_t thread_id,
                                                          ycsb_query *query) {
  set<uint64_t> all_keys;
  uint64_t max_row_id = config.table_size / config.num_partitions;
  uint32_t part_id = static_cast<uint32_t>(_random.nextInt64(thread_id) %
                                           config.num_partitions);
  uint64_t req_id = 0;
  for (uint32_t tmp = 0; tmp < YCSB_NUM_REQUESTS; tmp++) {
    ycsb_request *req = &(query->params.requests[req_id]);

    // Choose the access type based on random sequence from partition
    if (zipfian.nextRandDouble(part_id) < config.read_percent) {
      req->rtype = RD;
    } else {
      req->rtype = WR;
    }

    auto row_id = zipfian.nextZipfInt64(part_id);
    assert(row_id < max_row_id);

    req->key = row_id * config.num_partitions + part_id;
    req->value = static_cast<char>(zipfian.nextRandInt64(part_id) % (1 << 8));
    req->cc_info = 1;


    // Make sure a single row is not accessed twice
    if (all_keys.find(req->key) == all_keys.end()) {
      all_keys.insert(req->key);
      req_id++;
    } else {
      continue;
      // which means it will be overwritten!
    }
  }
  query->params.request_cnt = req_id;

  // Sort the requests in key order, if needed
  if (config.key_order) {
    auto num_reqs = static_cast<int>(query->params.request_cnt - 1);
    for (int i = num_reqs; i > 0; i--) {
      for (int j = 0; j < i; j++) {
        if (query->params.requests[j].key > query->params.requests[j + 1].key) {
          ycsb_request tmp = query->params.requests[j];
          query->params.requests[j] = query->params.requests[j + 1];
          query->params.requests[j + 1] = tmp;
        }
      }
    }

    for (int i = 0; i < num_reqs; i++) {
      assert(query->params.requests[i].key < query->params.requests[i + 1].key);
    }
  }
}

void YCSBWorkloadGenerator::gen_multi_partition_requests(uint64_t thread_id,
                                                         ycsb_query *query) {
  vector<uint32_t> parts;
  set<uint64_t> all_keys;

  // First generate all the parts that need to be accessed
  parts.reserve(YCSB_NUM_REQUESTS);
  for (uint32_t i = 0; i < YCSB_NUM_REQUESTS; i++) {
    auto rint64 = _random.nextInt64(thread_id);
    uint64_t part_id;
    if (_random.nextDouble(thread_id) < config.remote_access_percent) {
      part_id = (thread_id + 1) * config.num_local_partitions +
                (rint64 % config.num_remote_partitions);
      part_id = part_id % config.num_partitions;
    } else {
      part_id = (thread_id * config.num_local_partitions) +
                (rint64 % config.num_local_partitions);
    }
    parts.push_back(static_cast<uint32_t>(part_id));
  }

  uint64_t max_row_id = config.table_size / config.num_partitions;
  uint64_t req_id = 0;
  for (uint32_t tmp = 0; tmp < YCSB_NUM_REQUESTS; tmp++) {
    assert(req_id < YCSB_NUM_REQUESTS);
    ycsb_request *req = &(query->params.requests[req_id]);

    uint32_t part_id = parts[tmp];
    // Choose the access type
    if (zipfian.nextRandDouble(part_id) < config.read_percent) {
      req->rtype = RD;
    } else {
      req->rtype = WR;
    }

    auto row_id = zipfian.nextZipfInt64(part_id);
    assert(row_id < max_row_id);
    req->key = row_id * config.num_partitions + part_id;
    req->value = static_cast<char>(zipfian.nextRandInt64(part_id) % (1 << 8));

    // Make sure a single row is not accessed twice
    if (all_keys.find(req->key) == all_keys.end()) {
      all_keys.insert(req->key);
      req_id++;
    } else {
      continue;
    }
  }
  query->params.request_cnt = req_id;

  // Sort the requests in key order, if needed
  if (config.key_order) {
    auto num_reqs = static_cast<int>(query->params.request_cnt - 1);
    for (int i = num_reqs; i > 0; i--) {
      for (int j = 0; j < i; j++) {
        if (query->params.requests[j].key > query->params.requests[j + 1].key) {
          ycsb_request tmp = query->params.requests[j];
          query->params.requests[j] = query->params.requests[j + 1];
          query->params.requests[j + 1] = tmp;
        }
      }
    }

    for (int i = 0; i < num_reqs; i++) {
      assert(query->params.requests[i].key < query->params.requests[i + 1].key);
    }
  }
}

YCSBExecutor::YCSBExecutor(const YCSBBenchmarkConfig &config,
                           const string &folder_path, uint64_t num_threads)
    : BenchmarkExecutor<ycsb_params>(folder_path, num_threads), _db(config),
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
