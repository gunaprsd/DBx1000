// Copyright [2017] <Guna Prasaad>

#include "scheduler.h"
#include "config.h"
#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "thread.h"
#include "tpcc_database.h"
#include "tpcc_workload.h"
#include "ycsb_database.h"
#include "ycsb_workload.h"
#include "partitioner.h"

void parser(int argc, char **argv);

void partition() {
  string src_folder_path = get_benchmark_path(g_data_folder, false);
  string dst_folder_path = get_benchmark_path(g_data_folder, true);

  if (strcmp(g_benchmark, "ycsb") == 0) {
    if (g_task_type == PARTITION_DATA) {
      OfflineScheduler<ycsb_params> scheduler(src_folder_path, g_thread_cnt, g_size, dst_folder_path);
      scheduler.schedule();
    } else {
      assert(false);
    }
  } else if (strcmp(g_benchmark, "tpcc") == 0) {
    if (g_task_type == PARTITION_DATA) {
      OfflineScheduler<tpcc_params> scheduler(src_folder_path, g_thread_cnt, g_size, dst_folder_path);
      scheduler.schedule();
    } else {
      assert(false);
    }
  } else {
    assert(false);
  }
}

void generate() {
  if (strcmp(g_benchmark, "ycsb") == 0) {
    struct YCSBWorkloadConfig config = {
        .table_size = g_synth_table_size,
        .zipfian_theta = g_zipf_theta,
        .read_percent = g_read_perc,
        .num_partitions = g_part_cnt,
        .multi_part_txns_percent = g_perc_multi_part,
        .num_local_partitions = g_local_partitions,
        .remote_access_percent = g_remote_perc,
        .num_remote_partitions = g_remote_partitions};

    YCSBWorkloadGenerator generator(config, g_thread_cnt, g_size_per_thread,
                                    get_benchmark_path(g_data_folder, false));
    generator.generate();
    generator.release();
  } else if (strcmp(g_benchmark, "tpcc") == 0) {
    TPCCWorkloadGenerator generator(g_thread_cnt, g_size_per_thread,
                                    get_benchmark_path(g_data_folder, false));
    generator.generate();
    generator.release();
  }
}

void execute() {
  string folder_path;
  if (g_task_type == EXECUTE_RAW) {
    folder_path = get_benchmark_path(g_data_folder, false);
  } else if (g_task_type == EXECUTE_PARTITIONED) {
    folder_path = get_benchmark_path(g_data_folder, true);
  } else {
    assert(false);
  }

  if (strcmp(g_benchmark, "ycsb") == 0) {
    YCSBExecutor executor;
    executor.initialize(folder_path, g_thread_cnt);
    executor.execute();
    executor.release();
  } else if (strcmp(g_benchmark, "tpcc") == 0) {
    TPCCExecutor executor;
    executor.initialize(folder_path, g_thread_cnt);
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

  assert(g_size == g_thread_cnt * g_size_per_thread);
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
