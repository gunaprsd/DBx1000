// Copyright[2017] <Guna Prasaad>

#include <vector>
#include <stdint.h>
#include "global.h"
#include "helper.h"
#include "query.h"
#include "graph_partitioner.h"

#ifndef WORKLOAD_GENERATOR_H_
#define WORKLOAD_GENERATOR_H_

/*
 * ParallelWorkloadGenerator:
 * --------------------------
 * Generates a workload (in parallel) based on config and command line arguments
 * If a _folder_path is specified, the queries are written onto k binary files
 * with the format <_folder_path>/core_<i>.dat
 */
class ParallelWorkloadGenerator {
 public:
  virtual void initialize(uint32_t num_threads,
                          uint64_t num_params_per_thread,
                          const char * folder_path);
  virtual void release();
  void generate();
 protected:
  static  void *  generate_helper(void *ptr);

  uint32_t _num_threads;
  uint64_t _num_queries_per_thread;
  char _folder_path[200];

/* Need to be implemented by benchmark */
 public:
  virtual BaseQueryList *     get_queries_list(uint32_t thread_id) = 0;
  virtual BaseQueryMatrix *   get_queries_matrix() = 0;

 protected:
  virtual void per_thread_generate(uint32_t thread_id) = 0;
  virtual void per_thread_write_to_file(uint32_t thread_id, FILE * file) = 0;
};

/*
 * ParallelWorkloadLoader:
 * ------------------------
 * Loads k binary files of the form <_folder_path>/core_<i>.dat that each contain
 * queries in the binary format. The loaded queries can be obtained using
 * get_queries_list.
 */
class ParallelWorkloadLoader {
 public:
  virtual void initialize(uint32_t num_threads,
                          const char * base_file_name);
  virtual void release();
  void load();
 protected:
  static void * load_helper(void * ptr);
  uint32_t _num_threads;
  char _folder_path[200];

/* Need to be implemented by benchmark */
 public:
  virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
  virtual BaseQueryMatrix * get_queries_matrix() = 0;
 protected:
  virtual void per_thread_load(uint32_t thread_id, FILE * file) = 0;
};


#endif  // WORKLOAD_GENERATOR_H_
