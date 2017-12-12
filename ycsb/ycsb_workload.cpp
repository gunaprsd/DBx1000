// Copyright[2017] <Guna Prasaad>

#include "ycsb_workload.h"
#include <algorithm>

YCSBWorkloadGenerator::YCSBWorkloadGenerator(const YCSBWorkloadConfig &config,
                                             uint32_t num_threads,
                                             uint64_t num_queries_per_thread,
                                             const string &folder_path)
    : ParallelWorkloadGenerator(num_threads, num_queries_per_thread,
                                folder_path),
      _config(config),
      _zipf_generator(config.table_size / config.num_partitions, num_threads,
                      config.zipfian_theta),
      _rand_generator(num_threads) {

  _queries = new ycsb_query *[_num_threads];
  for (uint32_t i = 0; i < _num_threads; i++) {
    _queries[i] = new ycsb_query[_num_queries_per_thread];
  }
}

void YCSBWorkloadGenerator::gen_requests(uint32_t thread_id,
                                         ycsb_query *query) {
  vector<uint64_t> parts;
  set<uint64_t> all_keys;

  double r = _rand_generator.nextDouble(thread_id);

  // Choose multi or single partition transaction
  uint32_t num_parts;
  if(r < _config.multi_part_percent) {
    num_parts = _config.parts_per_txn;
  } else {
    num_parts = 1;
  }

  // Generate the parts that need to be accessed
  parts.reserve(num_parts);
  for(uint32_t i = 0; i < num_parts; i++) {
    uint64_t rint64 = _rand_generator.nextInt64(thread_id);
    uint64_t part_id = rint64 % _config.num_partitions;
    if(find(parts.begin(), parts.end(), part_id) != parts.end()) {
      parts.push_back(part_id);
    }
  }


  uint64_t max_row_id = _config.table_size / _config.num_partitions;
  uint64_t req_id = 0;
  for (uint32_t tmp = 0; tmp < MAX_REQ_PER_QUERY; tmp++) {
    assert(req_id < MAX_REQ_PER_QUERY);
    ycsb_request *req = &(query->params.requests[req_id]);

    // Choose the access type
    r = _rand_generator.nextDouble(thread_id);
    if (r < _config.read_percent) {
      req->rtype = RD;
    } else {
      req->rtype = WR;
    }

    auto part_id = static_cast<uint64_t>(((double)tmp / (double)MAX_REQ_PER_QUERY) * parts.size());
    auto row_id = _zipf_generator.nextInt64(thread_id);
    assert(row_id < max_row_id);
    req->key = row_id * _config.num_partitions + part_id;
    req->value = static_cast<char>(_rand_generator.nextInt64(thread_id) % (1 << 8));

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
  if (g_key_order) {
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

  assert(query->params.request_cnt <= MAX_REQ_PER_QUERY);
}

BaseQueryList *YCSBWorkloadGenerator::get_queries_list(uint32_t thread_id) {
  auto tquery_list = new QueryList<ycsb_params>();
  tquery_list->initialize(_queries[thread_id], _num_queries_per_thread);
  return tquery_list;
}

BaseQueryMatrix *YCSBWorkloadGenerator::get_queries_matrix() {
  auto matrix = new QueryMatrix<ycsb_params>();
  matrix->initialize(_queries, _num_threads, _num_queries_per_thread);
  return matrix;
}

void YCSBWorkloadGenerator::per_thread_generate(uint32_t thread_id) {
  _zipf_generator.seed(thread_id, thread_id + 1);
  _rand_generator.seed(thread_id, 2 * thread_id + 1);
  for (uint64_t i = 0; i < _num_queries_per_thread; i++) {
    gen_requests(thread_id, &(_queries[thread_id][i]));
  }
}

void YCSBWorkloadGenerator::per_thread_write_to_file(uint32_t thread_id,
                                                     FILE *file) {
  ycsb_query *thread_queries = _queries[thread_id];
  fwrite(thread_queries, sizeof(ycsb_query), _num_queries_per_thread, file);
}

BaseQueryList *YCSBWorkloadLoader::get_queries_list(uint32_t thread_id) {
  auto tquery_list = new QueryList<ycsb_params>();
  tquery_list->initialize(_queries[thread_id], _array_sizes[thread_id]);
  return tquery_list;
}

void YCSBWorkloadLoader::per_thread_load(uint32_t thread_id, FILE *file) {
  fseek(file, 0, SEEK_END);
  auto bytes_to_read = static_cast<size_t>(ftell(file));
  fseek(file, 0, SEEK_SET);

  _array_sizes[thread_id] =
      static_cast<uint32_t>(bytes_to_read / sizeof(ycsb_query));
  _queries[thread_id] =
      reinterpret_cast<ycsb_query *>(_mm_malloc(bytes_to_read, 64));

  size_t records_read = fread(_queries[thread_id], sizeof(ycsb_query),
                              _array_sizes[thread_id], file);
  assert(records_read == _array_sizes[thread_id]);
}

void YCSBWorkloadLoader::initialize(uint32_t num_threads,
                                    const char *folder_path) {
  ParallelWorkloadLoader::initialize(num_threads, folder_path);
  _queries = new ycsb_query *[_num_threads];
  _array_sizes = new uint32_t[_num_threads];
}

BaseQueryMatrix *YCSBWorkloadLoader::get_queries_matrix() {
  uint32_t const_size = _array_sizes[0];
  for (uint32_t i = 0; i < _num_threads; i++) {
    assert(_array_sizes[i] == const_size);
  }

  auto tquery_matrix = new QueryMatrix<ycsb_params>();
  tquery_matrix->initialize(_queries, _num_threads, const_size);
  return tquery_matrix;
}



void YCSBExecutor::initialize(uint32_t num_threads, const char *path) {
  BenchmarkExecutor::initialize(num_threads, path);

  // Build database in parallel
  _db = new YCSBDatabase();
  _db->initialize(INIT_PARALLELISM);
  _db->load();

  // Load workload in parallel
  _loader = new YCSBWorkloadLoader();
  _loader->initialize(num_threads, _path);
  _loader->load();

  // Initialize each thread
  for (uint32_t i = 0; i < _num_threads; i++) {
    _threads[i].initialize(i, _db, _loader->get_queries_list(i), true);
  }
}