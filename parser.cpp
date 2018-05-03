#include "global.h"
#include "helper.h"

#include <gflags/gflags.h>

/*
 * YCSB Command Line Arguments
 */
DEFINE_uint64(ycsb_table_size, YCSB_TABLE_SIZE, "YCSB: Table size");
DEFINE_double(ycsb_zipf_theta, YCSB_ZIPF_THETA, "YCSB: Zipfian theta for intra-partition distribution");
DEFINE_double(ycsb_read_percent, YCSB_READ_PERCENT, "YCSB: Read percentage");
DEFINE_uint32(ycsb_num_partitions, YCSB_PART_COUNT, "YCSB: Number of partitions");
DEFINE_double(ycsb_multipart_txns, YCSB_MULTIPART_PERCENT, "YCSB: Percentage of multi-part read_txns");
DEFINE_uint32(ycsb_num_local, YCSB_NUM_LOCAL, "YCSB: Number of local partitions");
DEFINE_uint32(ycsb_num_remote, YCSB_NUM_REMOTE, "YCSB: Number of remote partitions");
DEFINE_bool(ycsb_do_compute, YCSB_DO_COMPUTE, "YCSB: Do compute?");
DEFINE_uint32(ycsb_compute_cost, YCSB_COMPUTE_COST, "YCSB: Dummy operation cost");
DEFINE_double(ycsb_remote_percent, YCSB_REMOTE_PERCENT, "YCSB: Remote access percent");
DEFINE_bool(ycsb_key_order, YCSB_KEY_ORDER, "YCSB: Order key access?");

/*
 * TPCC Command Line Arguments
 */
DEFINE_bool(tpcc_wh_update, TPCC_WH_UPDATE, "TPCC: Update warehouse on payments?");
DEFINE_uint32(tpcc_num_wh, TPCC_NUM_WH, "TPCC: Number of Warehouses");
DEFINE_uint64(tpcc_max_items, TPCC_MAX_ITEMS, "TPCC: Max number of items");
DEFINE_uint64(tpcc_cust_per_dist, TPCC_CUST_PER_DIST, "TPCC: Customers per district");
DEFINE_uint64(tpcc_dist_per_wh, TPCC_DIST_PER_WH, "TPCC: Districts per warehouse");
DEFINE_double(tpcc_perc_payment, TPCC_PERC_PAYMENT, "TPCC: Percentage of payment read_txns");
DEFINE_double(tpcc_by_last_name_percent, TPCC_BY_LAST_NAME_PERC, "TPCC: Percent of payment read_txns with query by last name");
DEFINE_double(tpcc_remote_payment_percent, TPCC_REMOTE_PAYMENT_PERC, "TPCC: Percent of payment read_txns where customer is remote");

/*
 * General Command Line Arguments
 */
DEFINE_uint32(seed, 123, "Seed for generation");
DEFINE_string(task, "generate", "Choose task type: generate, partition, execute");
DEFINE_uint32(load_parallelism, INIT_PARALLELISM, "Parallelism for loading database");
DEFINE_uint64(size_per_thread, (256 * 1024), "Size Per Thread");
DEFINE_string(benchmark, "ycsb", "Benchmark to run on:ycsb, tpcc");
DEFINE_string(tag, "foo", "Tag for the benchmark");
DEFINE_string(input_file, "data", "file to ");
DEFINE_uint32(threads, 4, "Number of threads");
DEFINE_bool(pin_threads, true, "Pin threads?");
DEFINE_bool(hyperthreading, false, "Enable hyper-threading?");
DEFINE_bool(abort_buffer, false, "Use an abort buffer?");
DEFINE_uint32(abort_buffer_size, ABORT_BUFFER_SIZE, "Size of abort buffer");
DEFINE_uint32(abort_penalty, ABORT_PENALTY, "Penalty in mus");

/*
 * Partitioner/Execution Command Line Arguments
 */
DEFINE_bool(unit_weights, false, "Unit weights or conflict weights?");
DEFINE_bool(stdev_partitioner, false, "Partition based on std of savings?");
DEFINE_uint32(ufactor, 5, "Load imbalance tolerance for METIS (1 + x/1000)");
DEFINE_uint32(iterations, 5, "Number of iterations");
DEFINE_uint32(kmeans_dim, 100, "Dimensions for K-Means clustering");
DEFINE_string(parttype, "access_graph", "Options: access_graph, approx, conflict_graph");
DEFINE_string(partitioner, "access_graph", "Options: access_graph, conflict_graph");
DEFINE_string(output_file, "data", "Output folder for partitioner");


DEFINE_bool(generate_partitioned, false, "Generate Partitioned Workload");
DEFINE_bool(pre_schedule_txns, true, "Schedule all txns before starting workers");
DEFINE_bool(scheduler_batch_sync, true, "Synchronization across batches for online _batch scheduler");
DEFINE_uint32(scheduler_delay, 1000, "Delay between scheduler and worker");
DEFINE_uint32(scheduler_batch_size, 1000000, "Batch size for scheduling");
DEFINE_string(scheduler_type, "simple_parallel_queues", "Options: simple_parallel_queues, parallel_queues, shared_queue, transaction_queue");
DEFINE_uint64(wait_time, 5, "Amount of time to wait before running the experiment!");
DEFINE_double(sampling_perc, 10.0, "Sampling percentage for edges");