#include "tpcc_partitioner.h"

BaseQueryList *
TPCCConflictGraphPartitioner::get_queries_list(uint32_t thread_id) {
  auto queryList = new QueryList<tpcc_params>();
  queryList->initialize(_partitioned_queries[thread_id],
                        _tmp_queries[thread_id].size());
  return queryList;
}

void TPCCConflictGraphPartitioner::initialize(BaseQueryMatrix *queries,
                                              uint64_t max_cluster_graph_size,
                                              uint32_t parallelism,
                                              const char *dest_folder_path) {
  ConflictGraphPartitioner::initialize(queries, max_cluster_graph_size,
                                       parallelism, dest_folder_path);
  _partitioned_queries = nullptr;
}

void TPCCConflictGraphPartitioner::partition() {
  ConflictGraphPartitioner::partition();

  uint64_t start_time, end_time;
  start_time = get_server_clock();
  _partitioned_queries = new tpcc_query *[_num_arrays];
  for (uint32_t i = 0; i < _num_arrays; i++) {
    _partitioned_queries[i] = (tpcc_query *)_mm_malloc(
        sizeof(tpcc_query) * _tmp_queries[i].size(), 64);
    uint32_t offset = 0;
    for (auto iter = _tmp_queries[i].begin(); iter != _tmp_queries[i].end();
         iter++) {
      memcpy(&_partitioned_queries[i][offset], *iter, sizeof(tpcc_query));
      offset++;
    }
  }
  end_time = get_server_clock();
  shuffle_duration += DURATION(end_time, start_time);
}

void TPCCConflictGraphPartitioner::per_thread_write_to_file(uint32_t thread_id,
                                                            FILE *file) {
  tpcc_query *thread_queries = _partitioned_queries[thread_id];
  uint32_t size = _tmp_array_sizes[thread_id];
  fwrite(thread_queries, sizeof(tpcc_query), size, file);
}

void TPCCAccessGraphPartitioner::initialize(BaseQueryMatrix *queries,
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

  // getting sizes
  wh_cnt = g_num_wh;
  district_cnt = DIST_PER_WARE * wh_cnt;
  customer_cnt = g_cust_per_dist * district_cnt;
  items_cnt = g_max_items;
  stocks_cnt = g_max_items * wh_cnt;

  // create data arrays
  _wh_info = new TxnDataInfo[wh_cnt];
  _district_info = new TxnDataInfo[district_cnt];
  _customer_info = new TxnDataInfo[customer_cnt];
  _items_info = new TxnDataInfo[items_cnt];
  _stocks_info = new TxnDataInfo[stocks_cnt];
}

void TPCCAccessGraphPartitioner::partition() {
  AccessGraphPartitioner::partition();
  _partitioned_queries = new tpcc_query *[_num_arrays];
  for (uint32_t i = 0; i < _num_arrays; i++) {
    _partitioned_queries[i] = reinterpret_cast<tpcc_query *>(
        _mm_malloc(sizeof(tpcc_query) * _tmp_queries[i].size(), 64));
    uint32_t offset = 0;
    for (auto query : _tmp_queries[i]) {
      auto typed_query = reinterpret_cast<tpcc_query *>(query);
      memcpy(&_partitioned_queries[i][offset], typed_query, sizeof(tpcc_query));
      offset++;
    }
  }
}

//Create meta data
void TPCCAccessGraphPartitioner::first_pass() {
  BaseQuery *query = nullptr;
  // data ids start from max_size as the first max_size nodes are transactions
  _current_total_num_vertices = _max_size;

  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<tpcc_query *>(query);

    if (query->type == TPCC_PAYMENT_QUERY) {
      auto params = reinterpret_cast<tpcc_payment_params *>(&typed_query->params);

      auto wh = get_wh_info(params->w_id);
      auto dist = get_dist_info(params->d_w_id, params->d_id);

      first_pass_helper(wh, txn_id, g_wh_update);
      first_pass_helper(dist, txn_id);

      if (!params->by_last_name) {
        auto cust = get_cust_info(params->c_w_id, params->c_d_id, params->c_id);
        first_pass_helper(cust, txn_id, true);
      }

    } else if (query->type == TPCC_NEW_ORDER_QUERY) {
      auto params = reinterpret_cast<tpcc_new_order_params *>(&typed_query->params);

      auto wh = get_wh_info(params->w_id);
      auto dist = get_dist_info(params->w_id, params->d_id);
      auto cust = get_cust_info(params->w_id, params->d_id, params->c_id);

      first_pass_helper(wh, txn_id);
      first_pass_helper(dist, txn_id);
      first_pass_helper(cust, txn_id, true);

      for (unsigned int i = 0; i < params->ol_cnt; i++) {
        auto item = get_item_info(params->items[i].ol_i_id);
        auto stock = get_stock_info(params->items[i].ol_supply_w_id,
                                    params->items[i].ol_i_id);
        first_pass_helper(item, txn_id);
        first_pass_helper(stock, txn_id, true);
      }

    } else {
      assert(false);
    }
  }
}

// Create txn portion of the graph
void TPCCAccessGraphPartitioner::second_pass() {
  xadj.reserve(_current_total_num_vertices + 1);
  vwgt.reserve(_current_total_num_vertices);
  adjncy.reserve(2 * _current_total_num_edges);
  adjwgt.reserve(2 * _current_total_num_edges);

  BaseQuery *query = nullptr;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    internal_get_query(txn_id, &query);
    auto typed_query = static_cast<tpcc_query *>(query);

    if (query->type == TPCC_PAYMENT_QUERY) {
      auto params = reinterpret_cast<tpcc_payment_params *>(&typed_query->params);
      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(3);

      auto wh = get_wh_info(params->w_id);
      auto dist = get_dist_info(params->d_w_id, params->d_id);

      second_pass_helper(wh);
      second_pass_helper(dist);

      if (!params->by_last_name) {
        auto cust = get_cust_info(params->c_w_id, params->c_d_id, params->c_id);
        second_pass_helper(cust);
      }

    } else if (query->type == TPCC_NEW_ORDER_QUERY) {
      auto params = reinterpret_cast<tpcc_new_order_params *>(&typed_query->params);
      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(3 + 2 * params->ol_cnt);

      auto wh = get_wh_info(params->w_id);
      auto dist = get_dist_info(params->w_id, params->d_id);
      auto cust = get_cust_info(params->w_id, params->d_id, params->c_id);

      second_pass_helper(wh);
      second_pass_helper(dist);
      second_pass_helper(cust);

      for (unsigned int i = 0; i < params->ol_cnt; i++) {
        auto item = get_item_info(params->items[i].ol_i_id);
        auto stock = get_stock_info(params->items[i].ol_supply_w_id,
                                    params->items[i].ol_i_id);
        second_pass_helper(item);
        second_pass_helper(stock);
      }

    } else {
      assert(false);
    }
  }
}

// Create data portion of the graph
void TPCCAccessGraphPartitioner::third_pass() {
  _next_data_id = _max_size;
  BaseQuery *query = nullptr;
  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<tpcc_query *>(query);

    if (query->type == TPCC_PAYMENT_QUERY) {
      auto params = reinterpret_cast<tpcc_payment_params *>(&typed_query->params);

      auto wh = get_wh_info(params->w_id);
      auto dist = get_dist_info(params->d_w_id, params->d_id);

      third_pass_helper(wh);
      third_pass_helper(dist);

      if (!params->by_last_name) {
        auto cust = get_cust_info(params->c_w_id, params->c_d_id, params->c_id);
        third_pass_helper(cust);
      }

    } else if (query->type == TPCC_NEW_ORDER_QUERY) {
      auto params = reinterpret_cast<tpcc_new_order_params *>(&typed_query->params);

      auto wh = get_wh_info(params->w_id);
      auto dist = get_dist_info(params->w_id, params->d_id);
      auto cust = get_cust_info(params->w_id, params->d_id, params->c_id);

      third_pass_helper(wh);
      third_pass_helper(dist);
      third_pass_helper(cust);

      for (unsigned int i = 0; i < params->ol_cnt; i++) {
        auto item = get_item_info(params->items[i].ol_i_id);
        auto stock = get_stock_info(params->items[i].ol_supply_w_id,
                                    params->items[i].ol_i_id);
        third_pass_helper(item);
        third_pass_helper(stock);
      }

    } else {
      assert(false);
    }
  }

  xadj.push_back(static_cast<idx_t>(adjncy.size()));
}

void TPCCAccessGraphPartitioner::compute_post_stats(idx_t *parts) {
	min_data_degree = static_cast<uint32_t>(1 << 31);
	max_data_degree = 0;
  min_cross_data_degree = static_cast<uint32_t>(1 << 31);
  max_cross_data_degree = 0;
  total_cross_core_access = 0;

  // warehouse
  for (int i = 0; i < wh_cnt; i++) {
    TxnDataInfo *info = &_wh_info[i];
		compute_stats_helper(parts, info);
  }

  // districts
  for (int i = 0; i < district_cnt; i++) {
    TxnDataInfo *info = &_district_info[i];
		compute_stats_helper(parts, info);
  }

  // customers
  for (int i = 0; i < customer_cnt; i++) {
    TxnDataInfo *info = &_customer_info[i];
		compute_stats_helper(parts, info);
  }

  // items
  for (int i = 0; i < items_cnt; i++) {
    TxnDataInfo *info = &_items_info[i];
		compute_stats_helper(parts, info);
  }

  // stocks
  for (int i = 0; i < stocks_cnt; i++) {
    TxnDataInfo *info = &_stocks_info[i];
		compute_stats_helper(parts, info);
  }
}

void TPCCAccessGraphPartitioner::partition_per_iteration() {
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
  printf("Size of adjncy: %lld\n", (long long int)adjncy.size());

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

  assert(graph->nvtxs == (idx_t)xadj.size() - 1 &&
         graph->nvtxs == (idx_t)vwgt.size());
  assert(graph->adjncy_size == (idx_t)adjncy.size() &&
         graph->adjncy_size == (idx_t)adjwgt.size());

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

void TPCCAccessGraphPartitioner::per_thread_write_to_file(uint32_t thread_id,
                                                          FILE *file) {
  tpcc_query *thread_queries = _partitioned_queries[thread_id];
  uint32_t size = _tmp_array_sizes[thread_id];
  fwrite(thread_queries, sizeof(tpcc_query), size, file);
}
