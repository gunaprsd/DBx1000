#ifndef DBX1000_SCHEDULER_H
#define DBX1000_SCHEDULER_H

#include "loader.h"
#include "partitioner.h"

template <typename T> class OfflineScheduler {
public:
  OfflineScheduler(string input_folder_path, uint32_t num_threads,
                   uint64_t max_batch_size, string output_folder_path)
      : _input_folder_path(input_folder_path),
        _output_folder_path(output_folder_path), _num_threads(num_threads),
        _max_batch_size(max_batch_size),
        _loader(input_folder_path, num_threads), _partitioner(num_threads) {
    _loader.load();
    _original_queries = _loader.get_queries_matrix();
    _partitioned_queries = nullptr;

    _temp = new vector<Query<T> *>[_num_threads];
    _temp_sizes = new size_t[_num_threads];
    for (uint32_t i = 0; i < _num_threads; i++) {
      _temp_sizes[i] = 0;
    }

    _current_frame_offset = 0;
    _max_frame_offset = _original_queries->num_rows;
    _frame_height = _max_batch_size / _num_threads;
    assert(_max_batch_size % _num_threads == 0);
  }

  void schedule() {
    vector<idx_t> partitions;

    while (_current_frame_offset < _max_frame_offset) {
      uint64_t cframe_start = _current_frame_offset;
      uint64_t cframe_end =
          min(_current_frame_offset + _frame_height, _max_frame_offset);

      // Create batch from current frame and invoke partitioner
      QueryBatch<T> batch(_original_queries, cframe_start, cframe_end);
      _partitioner.partition(&batch, partitions);

      // Print some statistics, if necessary
      _partitioner.print_stats();

      // Shuffle queries into temp array
      uint64_t size = batch.size();
      for (uint64_t i = 0; i < size; i++) {
        auto query = batch[i];
        _temp[partitions[i]].push_back(query);
      }

      // Update temp array size parameters
      for (uint32_t i = 0; i < _num_threads; i++) {
        _temp_sizes[i] = _temp[i].size();
      }

      partitions.clear();
      _current_frame_offset += _frame_height;
    }

    uint64_t min_batch_size = UINT64_MAX;
    uint64_t max_batch_size = 0;
    for (uint32_t i = 0; i < _num_threads; i++) {
      min_batch_size = min(min_batch_size, _temp_sizes[i]);
      max_batch_size = max(max_batch_size, _temp_sizes[i]);
    }
    PRINT_INFO(lu, "Min-Batch-Size", min_batch_size);
    PRINT_INFO(lu, "Max-Batch-Size", max_batch_size);

    // write back onto arrays
    _partitioned_queries = new Query<T> *[_num_threads];
    for (uint32_t i = 0; i < _num_threads; i++) {
      _partitioned_queries[i] = reinterpret_cast<Query<T> *>(
          _mm_malloc(sizeof(Query<T>) * _temp[i].size(), 64));
      uint32_t offset = 0;
      for (auto query : _temp[i]) {
        auto tquery = reinterpret_cast<Query<T> *>(query);
        memcpy(&_partitioned_queries[i][offset], tquery, sizeof(Query<T>));
        offset++;
      }
    }

    // Write back onto files
    ensure_folder_exists(_output_folder_path);
    for (auto i = 0u; i < _num_threads; i++) {
      auto file_name = get_workload_file_name(_output_folder_path, i);
      FILE *file = fopen(file_name.c_str(), "w");
      if (file == nullptr) {
        printf("Unable to open file %s\n", file_name.c_str());
        exit(0);
      }

      Query<T> *thread_queries = _partitioned_queries[i];
      auto size = _temp_sizes[i];
      fwrite(thread_queries, sizeof(Query<T>), size, file);
      fflush(file);
      fclose(file);

#ifdef PRINT_CLUSTERED_FILE
			auto txt_file_name = file_name.substr(0, file_name.length() - 4);
			txt_file_name += ".txt";
			FILE *txt_file = fopen(txt_file_name.c_str(), "w");
			if (txt_file == nullptr) {
				printf("Unable to open file %s\n", txt_file_name.c_str());
				exit(0);
			}

      for (uint64_t j = 0; j < _temp_sizes[i]; j++) {
        print_query(txt_file, &_partitioned_queries[i][j]);
      }
      fflush(txt_file);
      fclose(txt_file);
    }
#endif

  }
  ~OfflineScheduler() {}

protected:
  const string _input_folder_path;
  const string _output_folder_path;
  const uint32_t _num_threads;
  const uint64_t _max_batch_size;
  ParallelWorkloadLoader<T> _loader;
  AccessGraphPartitioner<T> _partitioner;
  QueryMatrix<T> *_original_queries;
  Query<T> **_partitioned_queries;
  vector<Query<T> *> *_temp;
  size_t *_temp_sizes;
  uint64_t _current_frame_offset;
  uint64_t _max_frame_offset;
  uint64_t _frame_height;
};

#endif // DBX1000_SCHEDULER_H
