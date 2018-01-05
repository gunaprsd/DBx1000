#include "tpcc_partitioner.h"
#include "tpcc_helper.h"

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
	if(system(cmd)) {
		printf("Folder %s created!", dest_folder_path);
	}
	delete cmd;
	_partitioned_queries = nullptr;

	//getting sizes
	wh_cnt = g_num_wh;
	district_cnt = DIST_PER_WARE * wh_cnt;
	customer_cnt = g_cust_per_dist * district_cnt;
	items_cnt = g_max_items;
	stocks_cnt = g_max_items * wh_cnt;

	//create data arrays
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

// Add edges to data nodes
// Compute stats
void TPCCAccessGraphPartitioner::first_pass() {
  BaseQuery *query = nullptr;
  // data ids start from max_size as the first max_size nodes are transactions
  _current_total_num_vertices = _max_size;

  for (auto txn_id = 0u; txn_id < _max_size; txn_id++) {
    internal_get_query(txn_id, &query);
    auto typed_query = reinterpret_cast<tpcc_query *>(query);
    if (query->type == TPCC_PAYMENT_QUERY) {
      auto params =
          reinterpret_cast<tpcc_payment_params *>(&typed_query->params);

      TxnDataInfo *wh = &_wh_info[params->w_id];
      TxnDataInfo *dist = &_district_info[TPCCUtility::getDistrictKey(
          params->d_id, params->w_id)];
			add_internal_connection(wh, txn_id, g_wh_update);
			add_internal_connection(dist, txn_id);

      if (params->by_last_name) {
        TxnDataInfo *cust = &_customer_info[TPCCUtility::getCustomerPrimaryKey(
            params->c_id, params->c_d_id, params->c_w_id)];
				add_internal_connection(cust, txn_id, true);
      }

    } else if (query->type == TPCC_NEW_ORDER_QUERY) {
      auto params =
          reinterpret_cast<tpcc_new_order_params *>(&typed_query->params);

      TxnDataInfo *wh = &_wh_info[params->w_id];
      TxnDataInfo *dist = &_district_info[TPCCUtility::getDistrictKey(
          params->d_id, params->w_id)];
      TxnDataInfo *cust = &_customer_info[TPCCUtility::getCustomerPrimaryKey(
          params->c_id, params->d_id, params->w_id)];
			add_internal_connection(wh, txn_id, g_wh_update);
			add_internal_connection(dist, txn_id);
			add_internal_connection(cust, txn_id, true);

      for (unsigned int i = 0; i < params->ol_cnt; i++) {
        TxnDataInfo *item = &_items_info[params->items[i].ol_i_id];
        TxnDataInfo *stock = &_stocks_info[TPCCUtility::getStockKey(
            params->items[i].ol_i_id, params->items[i].ol_supply_w_id)];
				add_internal_connection(item, txn_id);
				add_internal_connection(stock, txn_id, true);
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
    auto typed_query = reinterpret_cast<tpcc_query *>(query);

    if (query->type == TPCC_PAYMENT_QUERY) {
      auto params =
          reinterpret_cast<tpcc_payment_params *>(&typed_query->params);
      xadj.push_back(static_cast<idx_t>(adjncy.size()));

      TxnDataInfo *wh = &_wh_info[params->w_id];
      adjncy.push_back(wh->id);
      adjwgt.push_back(1);

      TxnDataInfo *dist = &_district_info[TPCCUtility::getDistrictKey(
          params->d_id, params->w_id)];
      adjncy.push_back(dist->id);
      adjwgt.push_back(1);

      if (params->by_last_name) {
        TxnDataInfo *cust = &_customer_info[TPCCUtility::getCustomerPrimaryKey(
            params->c_id, params->c_d_id, params->c_w_id)];
        adjncy.push_back(cust->id);
        adjwgt.push_back(1);
        vwgt.push_back(3);
      } else {
        vwgt.push_back(2);
      }

    } else if (query->type == TPCC_NEW_ORDER_QUERY) {
      auto params =
          reinterpret_cast<tpcc_new_order_params *>(&typed_query->params);

      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(3 + 2 * params->ol_cnt);

      TxnDataInfo *wh = &_wh_info[params->w_id];
      adjncy.push_back(wh->id);
      adjwgt.push_back(1);

      TxnDataInfo *dist = &_district_info[TPCCUtility::getDistrictKey(
          params->d_id, params->w_id)];
      adjncy.push_back(dist->id);
      adjwgt.push_back(1);

      TxnDataInfo *cust = &_customer_info[TPCCUtility::getCustomerPrimaryKey(
          params->c_id, params->d_id, params->w_id)];
      adjncy.push_back(cust->id);
      adjwgt.push_back(1);

      for (unsigned int i = 0; i < params->ol_cnt; i++) {
        TxnDataInfo *item = &_items_info[params->items[i].ol_i_id];
        adjncy.push_back(item->id);
        adjwgt.push_back(1);

        TxnDataInfo *stock = &_stocks_info[TPCCUtility::getStockKey(
            params->items[i].ol_i_id, params->items[i].ol_supply_w_id)];
        adjncy.push_back(stock->id);
        adjwgt.push_back(1);
      }

    } else {
      assert(false);
    }
  }
}

// Create data portion of the graph
void TPCCAccessGraphPartitioner::third_pass() {
	min_data_degree = static_cast<uint32_t>(1 << 31);
	max_data_degree = 0;

  // warehouse
	for(int i = 0; i < wh_cnt; i++) {
		TxnDataInfo* info = &_wh_info[i];
		if(info->epoch == _current_iteration) {
			add_data_node(info);

		}
	}

	// districts
	for(int i = 0; i < district_cnt; i++) {
		TxnDataInfo* info = &_district_info[i];
		if(info->epoch == _current_iteration) {
			add_data_node(info);
		}
	}

	// customers
	for(int i = 0; i < customer_cnt; i++) {
		TxnDataInfo* info = &_wh_info[i];
		if(info->epoch == _current_iteration) {
			add_data_node(info);
		}
	}

	// items
	for(int i = 0; i < items_cnt; i++) {
		TxnDataInfo* info = &_items_info[i];
		if(info->epoch == _current_iteration) {
			add_data_node(info);
		}
	}

	// stocks
	for(int i = 0; i < stocks_cnt; i++) {
		TxnDataInfo* info = &_stocks_info[i];
		if(info->epoch == _current_iteration) {
			add_data_node(info);
		}
	}
}

void TPCCAccessGraphPartitioner::compute_post_stats(idx_t *parts) {
	min_cross_data_degree = 1 << 31;
	max_cross_data_degree = 0;
	total_cross_core_access = 0;

	// warehouse
	for(int i = 0; i < wh_cnt; i++) {
		TxnDataInfo* info = &_wh_info[i];
		if(info->epoch == _current_iteration) {
			compute_stats_helper(parts, info);
		}
	}

	// districts
	for(int i = 0; i < district_cnt; i++) {
		TxnDataInfo* info = &_district_info[i];
		if(info->epoch == _current_iteration) {
			compute_stats_helper(parts, info);
		}
	}

	// customers
	for(int i = 0; i < customer_cnt; i++) {
		TxnDataInfo* info = &_wh_info[i];
		if(info->epoch == _current_iteration) {
			compute_stats_helper(parts, info);
		}
	}

	// items
	for(int i = 0; i < items_cnt; i++) {
		TxnDataInfo* info = &_items_info[i];
		if(info->epoch == _current_iteration) {
			compute_stats_helper(parts, info);
		}
	}

	// stocks
	for(int i = 0; i < stocks_cnt; i++) {
		TxnDataInfo* info = &_stocks_info[i];
		if(info->epoch == _current_iteration) {
			compute_stats_helper(parts, info);
		}
	}

}

void TPCCAccessGraphPartitioner::partition_per_iteration() { assert(false); }

void TPCCAccessGraphPartitioner::per_thread_write_to_file(uint32_t thread_id,
                                                          FILE *file) {
  assert(false);
}
