// Copyright [2017] <Guna Prasaad>

#include "data_partitioner.h"

void DataPartitioner::initialize(BaseQueryMatrix *queries, uint64_t max_size,
                                 uint32_t parallelism,
                                 const char *dest_folder_path) {
  _original_queries = queries;
  _parallelism = parallelism;
  assert(_parallelism == 1);
  snprintf(_folder_path, sizeof(_folder_path), "%s", dest_folder_path);
  _num_arrays = _original_queries->num_arrays;
  _array_size = _original_queries->num_queries_per_array;

  _max_size = max_size;
  _max_size_per_array = _max_size / _num_arrays;

  // Simplistic assumption to eliminate corner cases
  assert(_array_size % _max_size_per_array == 0);
  assert(_max_size_per_array % _parallelism == 0);

  _tmp_queries = new vector<BaseQuery *>[_num_arrays];
  _tmp_array_sizes = new uint32_t[_num_arrays];
  for (auto i = 0u; i < _num_arrays; i++) {
    _tmp_array_sizes[i] = 0;
  }

  _current_iteration = 0;
  _current_total_num_edges = 0;
  _current_total_num_vertices = 0;

  _current_parts = reinterpret_cast<idx_t *>(malloc(sizeof(idx_t) * _max_size));
  for (auto i = 0u; i < _max_size; i++) {
    _current_parts[i] = -1;
  }
}


void DataPartitioner::write_to_files() {
  for (auto i = 0u; i < _num_arrays; i++) {
    char file_name[200];
    get_workload_file_name(_folder_path, i, file_name);
    FILE *file = fopen(file_name, "w");
    per_thread_write_to_file(i, file);
    fflush(file);
    fclose(file);
  }
}


void DataPartitioner::partition() {
  while (_current_array_start_offset < _array_size) {
    // Partition a graph of max_size
    partition_per_iteration();

    // Move to next iteration
    _current_iteration++;
    _current_array_start_offset = (_current_iteration * _max_size_per_array);
    for (auto i = 0u; i < _num_arrays; i++) {
      _tmp_array_sizes[i] = (uint32_t)_tmp_queries[i].size();
    }
  }
}
