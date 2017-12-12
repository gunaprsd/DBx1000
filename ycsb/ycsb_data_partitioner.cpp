#include <stdint.h>
#include "ycsb.h"


void YCSBDataPartitioner::initialize(BaseQueryMatrix *queries,
                                     uint64_t max_cluster_graph_size,
                                     uint32_t parallelism,
                                     const char *dest_folder_path) {
  DataPartitioner::initialize(queries, max_cluster_graph_size, parallelism,
                              dest_folder_path);
  _partitioned_queries = nullptr;
  _info_array = new TxnDataInfo[g_synth_table_size];
}

void YCSBDataPartitioner::partition() {
  DataPartitioner::partition();
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

void YCSBDataPartitioner::partition_per_iteration() {
  if (WRITE_PARTITIONS_TO_FILE) {
    write_pre_partition_file();
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
    get_query(i, &query);
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
    write_post_partition_file();
  }
}

void YCSBDataPartitioner::per_thread_write_to_file(uint32_t thread_id,
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
void YCSBDataPartitioner::first_pass() {
  BaseQuery *query = nullptr;
  uint64_t data_id = _max_size;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    _current_total_num_vertices++;
    get_query(txn_id, &query);
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
void YCSBDataPartitioner::second_pass() {
  xadj.reserve(_current_total_num_vertices + 1);
  vwgt.reserve(_current_total_num_vertices);
  adjncy.reserve(2 * _current_total_num_edges);
  adjwgt.reserve(2 * _current_total_num_edges);

  BaseQuery *query = nullptr;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    get_query(txn_id, &query);
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
 * Since we allotted data ids serially, we just seek to the next data id
 * and add its portion information into the graph
 */
void YCSBDataPartitioner::third_pass() {
  min_data_degree = 1 << 31;
  max_data_degree = 0;
  idx_t next_data_id = _max_size;
  BaseQuery *query = nullptr;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    get_query(txn_id, &query);
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

        min_data_degree = min(min_data_degree, (uint32_t)info->txns.size());
        max_data_degree = max(max_data_degree, (uint32_t)info->txns.size());
        next_data_id++;
      }
    }
  }

  xadj.push_back(static_cast<idx_t>(adjncy.size()));
}

void YCSBDataPartitioner::compute_post_stats(idx_t *parts) {
  BaseQuery *query = nullptr;
  min_cross_data_degree = 1 << 31;
  max_cross_data_degree = 0;
  total_cross_core_access = 0;
  idx_t next_data_id = _max_size;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<ycsb_query *>(query);
    auto params = &typed_query->params;
    for (auto j = 0u; j < params->request_cnt; j++) {
      auto hash = get_hash(params->requests[j].key);
      auto info = &_info_array[hash];

      if (info->id == next_data_id) {
        uint32_t data_cross_degree = 0;
        idx_t data_cluster = parts[info->id];
        for (auto txn : info->txns) {
          if (parts[txn] != data_cluster) {
            data_cross_degree++;
	    total_cross_core_access++;
          }
        }
        min_cross_data_degree = min(min_cross_data_degree, data_cross_degree);
        max_cross_data_degree = max(max_cross_data_degree, data_cross_degree);
        next_data_id++;
      }
    }
  }
}
