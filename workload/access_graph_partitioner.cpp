// Copyright [2017] <Guna Prasaad>
#include <algorithm>
#include "access_graph_partitioner.h"

void AccessGraphPartitioner::initialize(BaseQueryMatrix *queries, uint64_t max_size,
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
  _current_array_start_offset = 0;

  first_pass_duration = 0.0;
  second_pass_duration = 0.0;
  third_pass_duration = 0.0;
  partition_duration = 0.0;

  pre_total_cross_core_access = 0;
  pre_min_cross_core_access = 0;
  pre_max_cross_core_access = 0;
  post_total_cross_core_access = 0;
  post_min_cross_core_access = 0;
  post_max_cross_core_access = 0;
}

void AccessGraphPartitioner::write_to_files() {
  for (auto i = 0u; i < _num_arrays; i++) {
    char file_name[200];
    get_workload_file_name(_folder_path, i, file_name);
    FILE *file = fopen(file_name, "w");
    if(file == nullptr) {
      printf("Unable to open file %s\n", file_name);
      exit(0);
    }
    per_thread_write_to_file(i, file);
    fflush(file);
    fclose(file);
  }
}

void AccessGraphPartitioner::partition() {
  while (_current_array_start_offset < _array_size) {
    printf("Beginning iteration %u\n", _current_iteration);
    // Partition a graph of max_size
    partition_per_iteration();

    // Move to nextZipfInt64 iteration
    _current_iteration++;
    _current_array_start_offset = (_current_iteration * _max_size_per_array);
    for (auto i = 0u; i < _num_arrays; i++) {
      _tmp_array_sizes[i] = (uint32_t)_tmp_queries[i].size();
    }
  }
}

void AccessGraphPartitioner::debug_write_post_partition_file() {
  char file_name[100];
  snprintf(file_name, sizeof(file_name), "post_partition_%d.txt",
           _current_iteration);

  FILE *post_partition_file = fopen(file_name, "w");
  for (auto i = 0u; i < _num_arrays; i++) {
    auto num_queries = _tmp_queries[i].size() - _tmp_array_sizes[i];
    fprintf(post_partition_file, "Core\t:%d\tNum Queries\t:%ld\n",
            static_cast<int32>(i), static_cast<int64>(num_queries));

    for (auto j = _tmp_array_sizes[i]; j < (uint32_t)_tmp_queries[i].size();
         j++) {
      auto query = reinterpret_cast<BaseQuery *>(_tmp_queries[i][j]);
      fprintf(post_partition_file, "Transaction Id: (%d, %d)\n",
              static_cast<int32>(i), static_cast<int32>(j));
      print_query(post_partition_file, query);
    }
    fprintf(post_partition_file, "\n");
  }

  fflush(post_partition_file);
  fclose(post_partition_file);
}

void AccessGraphPartitioner::debug_write_pre_partition_file() {
  char file_name[100];
  snprintf(file_name, sizeof(file_name), "pre_partition_%d.txt",
           _current_iteration);

  FILE *pre_partition_file = fopen(file_name, "w");
  for (auto i = 0u; i < _num_arrays; i++) {
    fprintf(pre_partition_file, "Core\t:%d\tNum Queries\t:%ld\n",
            static_cast<int32>(i), static_cast<int64>(_max_size_per_array));
    for (auto j = _current_iteration * _max_size_per_array;
         j < (_current_iteration + 1) * _max_size_per_array; j++) {
      BaseQuery *query;
      _original_queries->get(i, j, &query);
      fprintf(pre_partition_file, "Transaction Id: (%d, %d)\n",
              static_cast<int32>(i), static_cast<int32>(j));
      print_query(pre_partition_file, query);
    }
    fprintf(pre_partition_file, "\n");
  }
  fflush(pre_partition_file);
  fclose(pre_partition_file);
}

void AccessGraphPartitioner::print_execution_summary() {
  printf("%-30s: %10lf\n", "First-Pass-Duration", first_pass_duration);
  printf("%-30s: %10lf\n", "Second-Pass-Duration", second_pass_duration);
  printf("%-30s: %10lf\n", "Third-Pass-Duration", third_pass_duration);
  printf("%-30s: %10lf\n", "Partition-Duration", partition_duration);
}

void AccessGraphPartitioner::print_partition_summary() {
  printf("%-30s: %u\n", "Iteration", _current_iteration);
  printf("%-30s: %lu\n", "Num-Vertices", _current_total_num_vertices);
  printf("%-30s: %lu\n", "Num-Edges", _current_total_num_edges);
  printf("%-30s: %lu\n", "Pre-Total-Cross-Core-Access", pre_total_cross_core_access);
  printf("%-30s: %lu\n", "Pre-Min-Cross-Core-Access", pre_min_cross_core_access);
  printf("%-30s: %lu\n", "Pre-Max-Cross-Core-Access", pre_max_cross_core_access);
  printf("%-30s: %lu\n", "Post-Total-Cross-Core-Access", post_total_cross_core_access);
  printf("%-30s: %lu\n", "Post-Min-Cross-Core-Access", post_min_cross_core_access);
  printf("%-30s: %lu\n", "Post-Max-Cross-Core-Access", post_max_cross_core_access);

  int32_t min_partition_size = INT32_MAX;
  int32_t max_partition_size = INT32_MIN;
  for (auto i = 0u; i < _num_arrays; i++) {
    int32_t diff = static_cast<int32_t>(_tmp_queries[i].size() - _tmp_array_sizes[i]);
    min_partition_size = min(min_partition_size, diff);
    max_partition_size = max(max_partition_size, diff);
  }

  printf("%-30s: %d\n", "Min-Cluster-Size", min_partition_size);
  printf("%-30s: %d\n", "Max-Cluster-Size", max_partition_size);
}
