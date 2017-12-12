// Copyright [2017] <Guna Prasaad>

#include "config.h"
#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "thread.h"
#include "tpcc_partitioner.h"
#include "tpcc_database.h"
#include "tpcc_workload.h"
#include "ycsb_partitioner.h"
#include "ycsb_workload.h"
#include "ycsb_database.h"

void parser(int argc, char **argv);

void partition() {
  string src_folder_path = get_benchmark_path(false);
  string dst_folder_path = get_benchmark_path(true);

  if(strcmp(g_benchmark, "ycsb") == 0) {
    YCSBWorkloadLoader loader;
    loader.initialize(g_thread_cnt, src_folder_path.c_str());

    if(g_task_type == PARTITION_DATA) {
      YCSBAccessGraphPartitioner partitioner;
      partitioner.initialize(loader.get_queries_matrix(), g_max_nodes_for_clustering, 1, dst_folder_path.c_str());
      partitioner.partition();
      partitioner.write_to_files();
      partitioner.print_execution_summary();
    } else if(g_task_type == PARTITION_CONFLICT) {
      YCSBConflictGraphPartitioner partitioner;
      partitioner.initialize(loader.get_queries_matrix(), g_max_nodes_for_clustering, 1, dst_folder_path.c_str());
      partitioner.partition();
      partitioner.write_to_files();
      partitioner.print_execution_summary();
    } else {
      assert(false);
    }

    loader.release();
  } else if(strcmp(g_benchmark, "tpcc") == 0) {
    TPCCWorkloadLoader loader;
    loader.initialize(g_thread_cnt, src_folder_path.c_str());
    if(g_task_type == PARTITION_DATA) {
      assert(false);
    } else if(g_task_type == PARTITION_CONFLICT) {
      TPCCConflictGraphPartitioner partitioner;
      partitioner.initialize(loader.get_queries_matrix(), g_max_nodes_for_clustering, 1, dst_folder_path.c_str());
      partitioner.partition();
      partitioner.write_to_files();
      partitioner.print_execution_summary();
    } else {
      assert(false);
    }

    loader.release();
  } else {
    assert(false);
  }
}

void generate() {
  if (strcmp(g_benchmark, "ycsb") == 0) {
    YCSBWorkloadConfig config{g_synth_table_size, g_zipf_theta,
                              g_read_perc,        g_part_cnt,
                              g_perc_multi_part,  g_part_per_txn};

    YCSBWorkloadGenerator generator(config, g_thread_cnt, g_queries_per_thread,
                                    string(get_benchmark_path(false)));
    generator.generate();
    generator.release();
  } else if (strcmp(g_benchmark, "tpcc") == 0) {
    TPCCWorkloadGenerator generator(g_thread_cnt, g_queries_per_thread,
                                    string(get_benchmark_path(false)));
    generator.generate();
    generator.release();
  }
}

void execute() {
  string folder_path;
  if(g_task_type == EXECUTE_RAW) {
    folder_path = get_benchmark_path(false);
  } else if(g_task_type == EXECUTE_PARTITIONED) {
    folder_path = get_benchmark_path(true);
  } else {
    assert(false);
  }

  if(strcmp(g_benchmark, "ycsb") == 0) {
    YCSBExecutor executor;
    executor.initialize(g_thread_cnt, folder_path.c_str());
    executor.execute();
    executor.release();
  } else if(strcmp(g_benchmark, "tpcc") == 0) {
    TPCCExecutor executor;
    executor.initialize(g_thread_cnt, folder_path.c_str());
    executor.execute();
    executor.release();
  } else {
    assert(false);
  }
}

int main(int argc, char **argv) {
  parser(argc, argv);

  mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
  stats.init();

  glob_manager = reinterpret_cast<Manager *>(_mm_malloc(sizeof(Manager), 64));
  glob_manager->init();

  check_and_init_variables();

	switch (g_task_type) {
		case GENERATE:
			generate();
			break;
		case EXECUTE_RAW:
		case EXECUTE_PARTITIONED:
			execute();
			break;
		case PARTITION_DATA:
		case PARTITION_CONFLICT:
			partition();
			break;
	}

  return 0;
}
