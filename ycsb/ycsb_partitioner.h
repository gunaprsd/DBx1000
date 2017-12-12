// Copyright [2017] <Guna Prasaad>

#ifndef YCSB_YCSB_PARTITIONER_H_
#define YCSB_YCSB_PARTITIONER_H_

#include "access_graph_partitioner.h"
#include "conflict_graph_partitioner.h"
#include "ycsb.h"

class YCSBConflictGraphPartitioner : public ConflictGraphPartitioner {
public:
  void initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size,
                  uint32_t parallelism, const char *dest_folder_path) override;
  void partition() override;
  BaseQueryList *get_queries_list(uint32_t thread_id) override;

protected:
  static void *compute_data_info_helper(void *data);
  int compute_weight(BaseQuery *q1, BaseQuery *q2) override;
  void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;
  void compute_data_info() override;
  uint32_t get_hash(uint64_t primary_key) {
    return static_cast<uint32_t>(primary_key % _data_info_size);
  }

  ycsb_query **_partitioned_queries;
  DataInfo *_data_info;
  uint32_t _data_info_size;
};

class YCSBAccessGraphPartitioner : public AccessGraphPartitioner {
public:
  void initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size,
                  uint32_t parallelism, const char *dest_folder_path) override;
  void partition() override;

protected:
  void first_pass();
  void second_pass();
  void third_pass();
  void compute_post_stats(idx_t *parts);
  inline uint32_t get_hash(uint64_t key) { return static_cast<uint32_t>(key); }
  void partition_per_iteration() override;
  void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;

  ycsb_query **_partitioned_queries;

  TxnDataInfo *_info_array;

  vector<idx_t> vwgt;
  vector<idx_t> adjwgt;
  vector<idx_t> xadj;
  vector<idx_t> adjncy;
};

#endif // YCSB_YCSB_PARTITIONER_H_
