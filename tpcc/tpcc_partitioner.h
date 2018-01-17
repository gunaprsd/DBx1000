// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_PARTITIONER_H_
#define TPCC_TPCC_PARTITIONER_H_

#include "access_graph_partitioner.h"
#include "conflict_graph_partitioner.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include <algorithm>

#define UPDATE(tid, did)                                                       \
  pre_cross_access += random_parts[tid] != random_parts[did] ? 1 : 0;           \
  post_cross_access += parts[tid] != parts[did] ? 1 : 0;

class TPCCConflictGraphPartitioner : public ConflictGraphPartitioner {
public:
  void initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size,
                  uint32_t parallelism, const char *dest_folder_path) override;

  BaseQueryList *get_queries_list(uint32_t thread_id) override;
  void partition() override;

protected:
  void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;
  tpcc_query **_partitioned_queries;

private:
  int compute_weight(BaseQuery *bq1, BaseQuery *bq2) override {
    auto q1 = (tpcc_query *)bq1;
    auto q2 = (tpcc_query *)bq2;
    if (q1->type == TPCC_PAYMENT_QUERY && q2->type == TPCC_PAYMENT_QUERY) {
      return compute_weight((tpcc_payment_params *)&q1->params,
                            (tpcc_payment_params *)&q2->params);
    } else if (q1->type == TPCC_NEW_ORDER_QUERY &&
               q2->type == TPCC_NEW_ORDER_QUERY) {
      return compute_weight((tpcc_new_order_params *)&q1->params,
                            (tpcc_new_order_params *)&q2->params);
    } else if (q1->type == TPCC_PAYMENT_QUERY &&
               q2->type == TPCC_NEW_ORDER_QUERY) {
      return compute_weight((tpcc_payment_params *)&q1->params,
                            (tpcc_new_order_params *)&q2->params);
    } else {
      return compute_weight((tpcc_payment_params *)&q2->params,
                            (tpcc_new_order_params *)&q1->params);
    }
  }

  int compute_weight(tpcc_payment_params *q1, tpcc_payment_params *q2) {
    // Don't know if c_last and c_id match - so ignore!
    if (q1->w_id == q2->w_id) {
      if (q1->d_id == q2->d_id) {
        return 10;
      } else {
        return 9;
      }
    } else {
      return -1;
    }
  }

  int compute_weight(tpcc_new_order_params *q1, tpcc_new_order_params *q2) {
    int weight = -1;
    if (q1->w_id == q2->w_id) {
      if (q1->d_id == q2->d_id) {
        weight += 20;
      } else {
        weight += 19;
      }
    }

    for (uint32_t i = 0; i < q1->ol_cnt; i++) {
      for (uint32_t j = 0; j < q2->ol_cnt; j++) {
        if (q1->items[i].ol_supply_w_id == q2->items[i].ol_supply_w_id &&
            q1->items[i].ol_i_id == q2->items[i].ol_i_id) {
          weight += 1;
        }
      }
    }

    return weight;
  }

  int compute_weight(tpcc_payment_params *q1, tpcc_new_order_params *q2) {
    if (q1->w_id == q2->w_id) {
      if (q1->d_id == q2->d_id) {
        if (q1->c_id == q2->c_id) {
          return 10;
        } else {
          return 9;
        }
      } else {
        return 8;
      }
    } else {
      return -1;
    }
  }
};

class TPCCAccessGraphPartitioner : public AccessGraphPartitioner {
public:
  void initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size,
                  uint32_t parallelism, const char *dest_folder_path) override;
  void partition() override;

protected:
  void first_pass();
  void second_pass();
  void third_pass();
  void compute_post_stats(idx_t *parts);
  void partition_per_iteration() override;
  void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;

private:
  void init(TxnDataInfo *info) {
    if (info->epoch != _current_iteration) {
      info->epoch = _current_iteration;
      info->id = _current_total_num_vertices;
      info->num_writes = 0;
      info->num_reads = 0;
      info->txns.clear();
      _current_total_num_vertices++;
    }
  }

  TxnDataInfo *get_wh_info(uint64_t wid) {
    auto info = &_wh_info[wid % wh_cnt];
    init(info);
    return info;
  }

  TxnDataInfo *get_dist_info(uint64_t wid, uint64_t did) {
    auto key = TPCCUtility::getDistrictKey(did, wid);
    auto index = key % district_cnt;
    auto info = &_district_info[index];
    init(info);
    return info;
  }

  TxnDataInfo *get_cust_info(uint64_t wid, uint64_t did, uint64_t cid) {
    auto key = TPCCUtility::getCustomerPrimaryKey(cid, did, wid);
    auto index = key % customer_cnt;
    auto info = &_customer_info[index];
    init(info);
    return info;
  }

  TxnDataInfo *get_item_info(uint64_t iid) {
    auto index = iid % items_cnt;
    auto info = &_items_info[index];
    init(info);
    return info;
  }

  TxnDataInfo *get_stock_info(uint64_t wid, uint64_t iid) {
    auto key = TPCCUtility::getStockKey(iid, wid);
    auto index = key % stocks_cnt;
    auto info = &_stocks_info[index];
    init(info);
    return info;
  }

  void first_pass_helper(TxnDataInfo *info, idx_t txn_id, bool write = false) {
    assert(info->epoch == _current_iteration);

    _current_total_num_edges++;
    info->txns.push_back(txn_id);
    if (write) {
      info->num_writes++;
    } else {
      info->num_reads++;
    }
  }

  void second_pass_helper(TxnDataInfo *info) {
    adjncy.push_back(info->id);
    adjwgt.push_back(1);
  }

  void third_pass_helper(TxnDataInfo *info) {

    if (info->id == _next_data_id) {
      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(0);

      adjncy.insert(adjncy.end(), info->txns.begin(), info->txns.end());
      adjwgt.insert(adjwgt.end(), info->txns.size(), 1);

      _next_data_id++;
    }
  }

  void compute_stats_helper(idx_t *pre_parts, idx_t *post_parts, idx_t txn_id,
                            idx_t data_id) {
    if (pre_parts[txn_id] != pre_parts[data_id]) {
    }

    if (post_parts[txn_id] != post_parts[data_id]) {
    }
  }

  tpcc_query **_partitioned_queries;
  TxnDataInfo *_wh_info;
  TxnDataInfo *_district_info;
  TxnDataInfo *_customer_info;
  TxnDataInfo *_stocks_info;
  TxnDataInfo *_items_info;

  int wh_cnt;
  int district_cnt;
  int customer_cnt;
  int stocks_cnt;
  int items_cnt;

  vector<idx_t> vwgt;
  vector<idx_t> adjwgt;
  vector<idx_t> xadj;
  vector<idx_t> adjncy;

  idx_t _next_data_id;
};

#endif // TPCC_TPCC_PARTITIONER_H_
