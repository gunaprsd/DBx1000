#include "ycsb_partitioner.h"
#include <stdint.h>

void YCSBConflictGraphPartitioner::initialize(BaseQueryMatrix *queries,
                                              uint64_t max_cluster_graph_size,
                                              uint32_t parallelism,
                                              const char *dest_folder_path) {
  ConflictGraphPartitioner::initialize(queries, max_cluster_graph_size,
                                       parallelism, dest_folder_path);
  auto cmd = new char[300];
  snprintf(cmd, 300, "mkdir -p %s", dest_folder_path);
  if (system(cmd)) {
    printf("Folder %s created!", dest_folder_path);
  }
  delete cmd;
  _partitioned_queries = nullptr;
  _data_info_size = static_cast<uint32_t>(g_synth_table_size);
  _data_info = new DataInfo[_data_info_size];
  for (uint32_t i = 0; i < _data_info_size; i++) {
    _data_info[i].epoch = 0;
    _data_info[i].num_reads = 1;
    _data_info[i].num_writes = 1;
  }
}

BaseQueryList *
YCSBConflictGraphPartitioner::get_queries_list(uint32_t thread_id) {
  assert(_partitioned_queries != nullptr);
  auto tquery_list = new QueryList<ycsb_params>();
  tquery_list->initialize(_partitioned_queries[thread_id],
                          _tmp_queries[thread_id].size());
  return tquery_list;
}

int YCSBConflictGraphPartitioner::compute_weight(BaseQuery *q1, BaseQuery *q2) {
  assert(q1 != q2);
  bool conflict = false;
  int weight = 0;
  auto p1 = &reinterpret_cast<ycsb_query *>(q1)->params;
  auto p2 = &reinterpret_cast<ycsb_query *>(q2)->params;
  for (uint32_t i = 0; i < p1->request_cnt; i++) {
    auto r1 = &p1->requests[i];
    for (uint32_t j = 0; j < p2->request_cnt; j++) {
      auto r2 = &p2->requests[j];
      if ((r1->key == r2->key) && (r1->rtype == WR || r2->rtype == WR)) {
        int inc = 1;
        if (false) {
          auto info = &_data_info[get_hash(r1->key)];
          if (info->epoch == _current_iteration) {
            // double rcon = static_cast<double>(info->num_reads) /
            //                static_cast<double>(_max_size);
            // double wcon = static_cast<double>(info->num_writes) /
            //                static_cast<double>(_max_size);
            // double con = rcon + wcon;
            // inc = static_cast<int>(contention * 100.0);
          } else {
            // it should have been updated!
            assert(false);
          }
        }
        weight += inc;
        conflict = true;
        break;
      }
    }
  }
  return conflict ? weight : -1;
}

void YCSBConflictGraphPartitioner::partition() {
  ConflictGraphPartitioner::partition();

  uint64_t start_time, end_time;
  start_time = get_server_clock();
  _partitioned_queries = new ycsb_query *[_num_arrays];
  for (uint32_t i = 0; i < _num_arrays; i++) {
    _partitioned_queries[i] = reinterpret_cast<ycsb_query *>(
        _mm_malloc(sizeof(ycsb_query) * _tmp_queries[i].size(), 64));
    uint32_t offset = 0;
    for (auto query : _tmp_queries[i]) {
      auto tquery = reinterpret_cast<ycsb_query *>(query);
      memcpy(&_partitioned_queries[i][offset], tquery, sizeof(ycsb_query));
      offset++;
    }
  }
  end_time = get_server_clock();
  shuffle_duration += DURATION(end_time, start_time);
}

void YCSBConflictGraphPartitioner::per_thread_write_to_file(uint32_t thread_id,
                                                            FILE *file) {
  ycsb_query *thread_queries = _partitioned_queries[thread_id];
  uint32_t size = _tmp_array_sizes[thread_id];
  fwrite(thread_queries, sizeof(ycsb_query), size, file);
}

void YCSBConflictGraphPartitioner::compute_data_info() {
  ConflictGraphPartitioner::compute_data_info();

  auto threads = new pthread_t[_parallelism];
  auto data = new ThreadLocalData[_parallelism];

  for (uint32_t i = 0; i < _parallelism; i++) {
    data[i].fields[0] = (uint64_t)this;
    data[i].fields[1] = (uint64_t)i;
    pthread_create(&threads[i], nullptr, compute_data_info_helper,
                   reinterpret_cast<void *>(&data[i]));
  }

  for (uint32_t i = 0; i < _parallelism; i++) {
    pthread_join(threads[i], nullptr);
  }

  delete[] threads;
  delete[] data;
}

void *YCSBConflictGraphPartitioner::compute_data_info_helper(void *data) {
  auto thread_data = reinterpret_cast<ThreadLocalData *>(data);
  auto partitioner =
      reinterpret_cast<YCSBConflictGraphPartitioner *>(thread_data->fields[0]);
  auto thread_id = (uint32_t)thread_data->fields[1];

  uint64_t num_global_nodes = partitioner->_max_size;
  uint64_t num_local_nodes = num_global_nodes / partitioner->_parallelism;

  uint64_t start = thread_id * num_local_nodes;
  uint64_t end = (thread_id + 1) * num_local_nodes;

  BaseQuery *baseQuery = nullptr;
  ycsb_params *params = nullptr;
  for (uint64_t i = start; i < end; i++) {
    partitioner->get_query(i, &baseQuery);
    params = &reinterpret_cast<ycsb_query *>(baseQuery)->params;

    for (uint64_t j = 0; j < params->request_cnt; j++) {
      uint64_t key = params->requests[j].key;
      uint32_t hash = partitioner->get_hash(key);
      auto info = &partitioner->_data_info[hash];

      if (info->epoch != partitioner->_current_iteration) {
        uint64_t current_val = info->epoch;
        ATOM_CAS(info->epoch, current_val, partitioner->_current_iteration);
      }

      if (params->requests[j].rtype == RD) {
        ATOM_FETCH_ADD(info->num_reads, 1);
      } else {
        ATOM_FETCH_ADD(info->num_writes, 1);
      }
    }
  }

  return nullptr;
}

void YCSBAccessGraphPartitioner::initialize(BaseQueryMatrix *queries,
                                            uint64_t max_cluster_graph_size,
                                            uint32_t parallelism,
                                            const char *dest_folder_path) {
  AccessGraphPartitioner::initialize(queries, max_cluster_graph_size,
                                     parallelism, dest_folder_path);
  auto cmd = new char[300];
  snprintf(cmd, 300, "mkdir -p %s", dest_folder_path);
  if (system(cmd)) {
    printf("Folder %s created!", dest_folder_path);
  }
  delete cmd;
  _partitioned_queries = nullptr;
  _info_array = new TxnDataInfo[g_synth_table_size];
}

void YCSBAccessGraphPartitioner::partition() {
  AccessGraphPartitioner::partition();
  _partitioned_queries = new ycsb_query *[_num_arrays];
  for (uint32_t i = 0; i < _num_arrays; i++) {
    _partitioned_queries[i] = reinterpret_cast<ycsb_query *>(
        _mm_malloc(sizeof(ycsb_query) * _tmp_queries[i].size(), 64));
    uint32_t offset = 0;
    for (auto query : _tmp_queries[i]) {
      auto typed_query = reinterpret_cast<ycsb_query *>(query);
      memcpy(&_partitioned_queries[i][offset], typed_query, sizeof(ycsb_query));
      offset++;
    }
  }
}

void YCSBAccessGraphPartitioner::partition_per_iteration() {
  if (WRITE_PARTITIONS_TO_FILE) {
    debug_write_pre_partition_file();
  }

  uint64_t start_time, end_time;
  double duration;

  start_time = get_server_clock();
  first_pass();
  end_time = get_server_clock();
  duration = DURATION(end_time, start_time);
  first_pass_duration += duration;
  printf("First pass completed in %lf secs\n", duration);

  start_time = get_server_clock();
  second_pass();
  end_time = get_server_clock();
  duration = DURATION(end_time, start_time);
  second_pass_duration += duration;
  printf("Second pass completed in %lf secs\n", duration);

  start_time = get_server_clock();
  third_pass();
  end_time = get_server_clock();
  duration = DURATION(end_time, start_time);
  third_pass_duration += duration;
  printf("Third pass completed in %lf secs\n", duration);

  auto graph = new METIS_CSRGraph();
  graph->nvtxs = _current_total_num_vertices;
  graph->adjncy_size = 2 * _current_total_num_edges;
  graph->vwgt = vwgt.data();
  graph->xadj = xadj.data();
  graph->adjncy = adjncy.data();
  graph->adjwgt = adjwgt.data();
  graph->ncon = 1;

  auto parts = new idx_t[_current_total_num_vertices];
  for (uint32_t i = 0; i < _current_total_num_vertices; i++) {
    parts[i] = -1;
  }

  start_time = get_server_clock();
  METISGraphPartitioner::compute_partitions(graph, _num_arrays, parts);
  end_time = get_server_clock();
  duration = DURATION(end_time, start_time);
  partition_duration += duration;
  printf("Partition completed in %lf secs\n", duration);

  BaseQuery *query = nullptr;
  for (auto i = 0u; i < _max_size; i++) {
    auto partition = static_cast<int>(parts[i]);
    assert(partition != -1);
    internal_get_query(i, &query);
    _tmp_queries[partition].push_back(query);
  }

  compute_post_stats(parts);

  print_partition_summary();

  delete graph;
  delete[] parts;
  xadj.clear();
  vwgt.clear();
  adjncy.clear();
  adjwgt.clear();

  if (WRITE_PARTITIONS_TO_FILE) {
    debug_write_post_partition_file();
  }
}

void YCSBAccessGraphPartitioner::per_thread_write_to_file(uint32_t thread_id,
                                                          FILE *file) {
  ycsb_query *thread_queries = _partitioned_queries[thread_id];
  uint32_t size = _tmp_array_sizes[thread_id];
  fwrite(thread_queries, sizeof(ycsb_query), size, file);
}

/*
 * This is the first pass on the batch, achieves two purposes:
 * (1) assigns a batch-specific id to each data item that is being accessed in
 * the batch (2) computes statistics on data items
 */
void YCSBAccessGraphPartitioner::first_pass() {
  BaseQuery *query = nullptr;
  uint64_t data_id = _max_size;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    _current_total_num_vertices++;
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<ycsb_query *>(query);
    auto params = &typed_query->params;

    for (auto j = 0u; j < params->request_cnt; j++) {
      auto hash = get_hash(params->requests[j].key);
      auto info = &_info_array[hash];

      // allot a valid id for the data item
      if (info->epoch != _current_iteration) {
        info->id = data_id++;
        info->epoch = _current_iteration;
        _current_total_num_vertices++;
      }

      // increment data statistics
      if (params->requests[j].rtype == RD) {
        info->num_reads++;
      } else {
        info->num_writes++;
      }
    }
    _current_total_num_edges += params->request_cnt;
  }
}

/*
 * In the second pass, we compute all the D -> T edges locally
 * and create the transaction portion of the graph
 */
void YCSBAccessGraphPartitioner::second_pass() {
  xadj.reserve(_current_total_num_vertices + 1);
  vwgt.reserve(_current_total_num_vertices);
  adjncy.reserve(2 * _current_total_num_edges);
  adjwgt.reserve(2 * _current_total_num_edges);

  BaseQuery *query = nullptr;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<ycsb_query *>(query);
    auto params = &typed_query->params;

    xadj.push_back(static_cast<idx_t>(adjncy.size()));
    vwgt.push_back(params->request_cnt);

    for (auto j = 0u; j < params->request_cnt; j++) {
      auto hash = get_hash(params->requests[j].key);
      auto info = &_info_array[hash];

      if (info->txns.empty()) {
        info->txns.reserve(info->num_writes + info->num_reads);
      }
      info->txns.push_back(txn_id);

      // idx_t weight = info->num_reads + 2 * info->num_writes;
      adjncy.push_back(info->id);
      adjwgt.push_back(1);
    }
  }
}

/*
 * In the third pass, we create the data portion of the graph.
 * Since we allotted data ids serially, we just seek to the nextZipfInt64 data id
 * and add its portion information into the graph
 */
void YCSBAccessGraphPartitioner::third_pass() {
  idx_t next_data_id = _max_size;
  BaseQuery *query = nullptr;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<ycsb_query *>(query);
    auto params = &typed_query->params;

    for (auto j = 0u; j < params->request_cnt; j++) {
      auto hash = get_hash(params->requests[j].key);
      auto info = &_info_array[hash];

      if (info->id == next_data_id) {
        xadj.push_back(static_cast<idx_t>(adjncy.size()));
        vwgt.push_back(0);

        // idx_t weight = info->num_reads + 2 * info->num_writes;
        adjncy.insert(adjncy.end(), info->txns.begin(), info->txns.end());
        adjwgt.insert(adjwgt.end(), info->txns.size(), 1);

        next_data_id++;
      }
    }
  }
  xadj.push_back(static_cast<idx_t>(adjncy.size()));
}

void YCSBAccessGraphPartitioner::compute_post_stats(idx_t *parts) {
  pre_total_cross_core_access = 0;
  pre_max_cross_core_access = 0;
  pre_min_cross_core_access = UINT64_MAX;
  post_total_cross_core_access = 0;
  post_max_cross_core_access = 0;
  post_min_cross_core_access = UINT64_MAX;

  idx_t *random_parts = new idx_t[_current_total_num_vertices];
  for(size_t i = 0; i < _current_total_num_vertices; i++) {
    random_parts[i] = rand() % _num_arrays;
  }

  BaseQuery *query = nullptr;
  uint64_t pre_cross_access, post_cross_access;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    pre_cross_access = 0;
    post_cross_access = 0;
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<ycsb_query *>(query);
    auto params = &typed_query->params;
    for (auto j = 0u; j < params->request_cnt; j++) {
      auto hash = get_hash(params->requests[j].key);
      auto info = &_info_array[hash];

      if(random_parts[txn_id] != random_parts[info->id]) {
        pre_cross_access++;
      }

      if(parts[txn_id] != parts[info->id]) {
        post_cross_access++;
      }
    }

    pre_total_cross_core_access += pre_cross_access;
    pre_min_cross_core_access = min(pre_min_cross_core_access, pre_cross_access);
    pre_max_cross_core_access = max(pre_max_cross_core_access, pre_cross_access);

    post_total_cross_core_access += post_cross_access;
    post_min_cross_core_access = min(post_min_cross_core_access, post_cross_access);
    post_max_cross_core_access = max(post_max_cross_core_access, post_cross_access);
  }

  delete random_parts;
}
