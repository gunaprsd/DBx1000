// Copyright [2017] <Guna Prasaad>


#include "global.h"
#include "config.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "ycsb.h"

void parser(int argc, char * *argv);

int main(int argc, char* * argv) {
  parser(argc, argv);

  mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
  stats.init();

  glob_manager = reinterpret_cast<Manager *>(_mm_malloc(sizeof(Manager), 64));
  glob_manager->init();

  check_and_init_variables();

  ParallelWorkloadGenerator * generator = nullptr;
  ParallelWorkloadLoader * loader = nullptr;
  //ParallelWorkloadPartitioner * partitioner = nullptr;
  DataPartitioner * data_partitioner = nullptr;
  BenchmarkExecutor * executor = nullptr;

  if (strcmp(g_benchmark, "ycsb") == 0) {
    generator = new YCSBWorkloadGenerator();
    loader = new YCSBWorkloadLoader();
    //partitioner = new YCSBWorkloadPartitioner();
    data_partitioner = new YCSBDataPartitioner();
    executor = new YCSBExecutor();
  } else if (strcmp(g_benchmark, "tpcc") == 0) {
    generator = new TPCCWorkloadGenerator();
    loader = new TPCCWorkloadLoader();
    //partitioner = new TPCCWorkloadPartitioner();
    executor = new TPCCExecutor();
  }

  if (g_task_type == PARTITION) {
    loader->initialize(g_thread_cnt, get_benchmark_path(false));
    loader->load();
    data_partitioner->initialize(
                   loader->get_queries_matrix(),
                   g_max_nodes_for_clustering,
                   1,
                   get_benchmark_path(true));
    data_partitioner->partition();
    data_partitioner->write_to_files();
    data_partitioner->print_execution_summary();
    loader->release();
  } else if (g_task_type == GENERATE) {
    generator->initialize(g_thread_cnt,
                          g_queries_per_thread,
                          get_benchmark_path(false));
    generator->generate();
    generator->release();
  } else if (g_task_type == EXECUTE_RAW) {
    executor->initialize(g_thread_cnt, get_benchmark_path(false));
    executor->execute();
    executor->release();
  } else if (g_task_type == EXECUTE_PARTITIONED) {
    executor->initialize(g_thread_cnt, get_benchmark_path(true));
    executor->execute();
    executor->release();
  }

  return 0;
}
