// Copyright [2017] <Guna Prasaad>

#include "graph_partitioner.h"
#include "query.h"
#include <vector>

#ifndef WORKLOAD_DATA_PARTITIONER_H_
#define WORKLOAD_DATA_PARTITIONER_H_

class DataPartitioner {
public:
  virtual void initialize(BaseQueryMatrix *queries, uint64_t max_size,
                          uint32_t parallelism, const char *dest_folder_path);
  void write_to_files();
  void partition();

protected:
  // Iteration sensitive -> depends on value of _current_iteration
  void get_query(uint64_t qid, BaseQuery **query);
  uint32_t get_array_idx(uint64_t qid);

  uint32_t _parallelism;
  uint32_t _num_arrays;
  uint64_t _array_size;

  uint64_t _max_size;
  uint64_t _max_size_per_array;

  uint32_t _current_iteration;
  uint64_t _current_array_start_offset;
  uint64_t _current_total_num_edges;
  uint64_t _current_total_num_vertices;
  idx_t *_current_parts;

  BaseQueryMatrix *_original_queries;
  vector<BaseQuery *> *_tmp_queries;
  uint32_t *_tmp_array_sizes;
  char _folder_path[200];

  virtual void partition_per_iteration() = 0;
  virtual void per_thread_write_to_file(uint32_t thread_id, FILE *file) = 0;
};

inline void DataPartitioner::get_query(uint64_t qid, BaseQuery **query) {
  auto array_idx = static_cast<uint32_t>(qid % _num_arrays);
  auto array_offset =
      static_cast<uint32_t>((qid / _num_arrays) + _current_array_start_offset);
  _original_queries->get(array_idx, array_offset, query);
}

inline uint32_t DataPartitioner::get_array_idx(uint64_t qid) {
  return static_cast<uint32_t>(qid % _num_arrays);
}
#endif // WORKLOAD_DATA_PARTITIONER_H_
