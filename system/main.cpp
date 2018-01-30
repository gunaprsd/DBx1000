
#include "config.h"
#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "parser.h"
#include "partitioner.h"
#include "scheduler.h"
#include "thread.h"
#include "tpcc_database.h"
#include "tpcc_workload.h"
#include "ycsb_database.h"
#include "ycsb_workload.h"

void execute_ycsb_benchmark() {
  struct YCSBBenchmarkConfig config = {
      .table_size = FLAGS_ycsb_table_size,
      .zipfian_theta = FLAGS_ycsb_zipf_theta,
      .read_percent = FLAGS_ycsb_read_percent,
      .num_partitions = FLAGS_ycsb_num_partitions,
      .multi_part_txns_percent = FLAGS_ycsb_multipart_txns,
      .num_local_partitions = FLAGS_ycsb_num_local,
      .remote_access_percent = FLAGS_ycsb_remote_percent,
      .num_remote_partitions = FLAGS_ycsb_num_remote,
      .key_order = FLAGS_ycsb_key_order,
      .do_compute = FLAGS_ycsb_do_compute,
      .compute_cost = FLAGS_ycsb_compute_cost};
  YCSBUtility::initialize(config);
  if (FLAGS_task == "partition") {
    OfflineScheduler<ycsb_params> scheduler(
        FLAGS_input_folder, FLAGS_threads,
        FLAGS_threads * FLAGS_size_per_thread, FLAGS_output_folder);
    scheduler.schedule();
  } else if (FLAGS_task == "generate") {
    YCSBWorkloadGenerator generator(config, FLAGS_threads,
                                    FLAGS_size_per_thread, FLAGS_output_folder);
    generator.generate();
  } else if (FLAGS_task == "execute") {
    YCSBExecutor executor(config, FLAGS_input_folder, FLAGS_threads);
    executor.execute();
    executor.release();
  }
}

void execute_tpcc_benchmark() {
  struct TPCCBenchmarkConfig config = {
      .num_warehouses = FLAGS_tpcc_num_wh,
      .items_count = FLAGS_tpcc_max_items,
      .districts_per_warehouse = FLAGS_tpcc_dist_per_wh,
      .customers_per_district = FLAGS_tpcc_cust_per_dist,
      .percent_payment = FLAGS_tpcc_perc_payment,
      .warehouse_update = FLAGS_tpcc_wh_update};
  TPCCUtility::initialize(config);
  if (FLAGS_task == "partition") {
    OfflineScheduler<tpcc_params> scheduler(
        FLAGS_input_folder, FLAGS_threads,
        FLAGS_threads * FLAGS_size_per_thread, FLAGS_output_folder);
    scheduler.schedule();
  } else if (FLAGS_task == "generate") {
    TPCCWorkloadGenerator generator(config, FLAGS_threads,
                                    FLAGS_size_per_thread, FLAGS_output_folder);
    generator.generate();
  } else if (FLAGS_task == "execute") {
    TPCCExecutor executor(config, FLAGS_input_folder, FLAGS_threads);
    executor.execute();
    executor.release();
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
  stats.init();

  glob_manager = reinterpret_cast<Manager *>(_mm_malloc(sizeof(Manager), 64));
  glob_manager->init();

  if (FLAGS_benchmark == "ycsb") {
    execute_ycsb_benchmark();
  } else if (FLAGS_benchmark == "tpcc") {
    execute_tpcc_benchmark();
  }

  return 0;
}
