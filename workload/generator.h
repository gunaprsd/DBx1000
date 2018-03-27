
// Copyright[2017] <Guna Prasaad>

#include "global.h"
#include "graph_partitioner.h"
#include "helper.h"
#include "query.h"
#include <stdint.h>
#include <vector>

#ifndef WORKLOAD_GENERATOR_H_
#define WORKLOAD_GENERATOR_H_

//#define HUMAN_PRINT_WORKLOAD_DEBUG

/*
 * ParallelWorkloadGenerator:
 * --------------------------
 * Generates a workload (in parallel) based on _wl_config and command line
 * arguments If a _folder_path is specified, the queries are written onto k
 * binary files with the format <_folder_path>/core_<i>.dat
 */
template <typename T> class ParallelWorkloadGenerator {
public:
  ParallelWorkloadGenerator(uint64_t num_threads,
                            uint64_t num_queries_per_thread,
                            const string &folder_path)
      : _num_threads(num_threads),
        _num_queries_per_thread(num_queries_per_thread),
        _folder_path(folder_path) {
    ensure_folder_exists(_folder_path);
    _queries = new Query<T> *[_num_threads];
    for (uint32_t i = 0; i < _num_threads; i++) {
      _queries[i] = new Query<T>[_num_queries_per_thread];
      for(uint64_t j = 0; j < _num_queries_per_thread; j++) {
        _queries[i]->is_data_node = false;
      }
    }
  }
  QueryIterator<T> *get_query_iterator(uint64_t thread_id) {
    return new QueryIterator<T>(_queries[thread_id], _num_queries_per_thread);
  }
  QueryMatrix<T> *get_queries_matrix() {
    return new QueryMatrix<T>(_queries, _num_threads, _num_queries_per_thread);
  }
  void generate() {
    auto threads = new pthread_t[_num_threads];
    auto data = new ThreadLocalData[_num_threads];

    uint64_t start_time = get_server_clock();
    for (auto i = 0u; i < _num_threads; i++) {
      data[i].fields[0] = (uint64_t)this;
      data[i].fields[1] = (uint64_t)i;
      pthread_create(&threads[i], nullptr, generate_helper,
                     reinterpret_cast<void *>(&data[i]));
    }

    for (auto i = 0u; i < _num_threads; i++) {
      pthread_join(threads[i], nullptr);
    }
    uint64_t end_time = get_server_clock();
    double duration = DURATION(end_time, start_time);
    printf("Workload Generation Completed in %lf secs\n", duration);

#ifdef HUMAN_PRINT_WORKLOAD_DEBUG
    for (uint64_t i = 0; i < _num_threads; i++) {
      string file_name = get_workload_file_name(_folder_path, i);
      int pos = file_name.find(".");
      auto hfile_name = file_name.substr(0, pos);
      hfile_name += ".txt";
      FILE *file = fopen(hfile_name.c_str(), "w");
      if (file == nullptr) {
        printf("Error opening file: %s\n", file_name.c_str());
        exit(0);
      }
      for (uint64_t j = 0; j < _num_queries_per_thread; j++) {
	print_query(file, &_queries[i][j]);
      }
      fflush(file);
      fclose(file);
    }
#endif

    delete[] threads;
    delete[] data;
  }
  virtual ~ParallelWorkloadGenerator() = default;

protected:
  static void *generate_helper(void *ptr) {
    auto data = reinterpret_cast<ThreadLocalData *>(ptr);
    auto loader =
        reinterpret_cast<ParallelWorkloadGenerator *>(data->fields[0]);
    auto thread_id = (uint32_t)((uint64_t)data->fields[1]);

    loader->per_thread_generate(thread_id);

    // Obtain filename
    auto file_name = get_workload_file_name(loader->_folder_path, thread_id);
    FILE *file = fopen(file_name.c_str(), "w");
    if (file == nullptr) {
      printf("Error opening file: %s\n", file_name.c_str());
      exit(0);
    }
    loader->per_thread_write_to_file(thread_id, file);
    fflush(file);
    fclose(file);

    return nullptr;
  }
  void per_thread_write_to_file(uint64_t thread_id, FILE *file) {
    Query<T> *thread_queries = _queries[thread_id];
    fwrite(thread_queries, sizeof(Query<T>), _num_queries_per_thread, file);
  }
  virtual void per_thread_generate(uint64_t thread_id) = 0;

  // Data fields
  const uint64_t _num_threads;
  const uint64_t _num_queries_per_thread;
  const string _folder_path;
  Query<T> **_queries;
};

#endif // WORKLOAD_GENERATOR_H_
