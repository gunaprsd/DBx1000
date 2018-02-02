#ifndef SYSTEM_PARSER_H__
#define SYSTEM_PARSER_H__

#include <gflags/gflags.h>
/*
 * YCSB Command Line Arguments
 */
DECLARE_uint64(ycsb_table_size);
DECLARE_double(ycsb_zipf_theta);
DECLARE_double(ycsb_read_percent);
DECLARE_uint32(ycsb_num_partitions);
DECLARE_double(ycsb_multipart_txns);
DECLARE_double(ycsb_remote_percent);
DECLARE_uint32(ycsb_num_local);
DECLARE_uint32(ycsb_num_remote);
DECLARE_bool(ycsb_do_compute);
DECLARE_uint32(ycsb_compute_cost);
DECLARE_double(ycsb_remote_percent);
DECLARE_bool(ycsb_key_order);

/*
 * TPCC Command Line Arguments
 */
DECLARE_uint32(tpcc_num_wh);
DECLARE_bool(tpcc_wh_update);
DECLARE_double(tpcc_perc_payment);
DECLARE_uint64(tpcc_max_items);
DECLARE_uint64(tpcc_cust_per_dist);
DECLARE_uint64(tpcc_dist_per_wh);
DECLARE_double(tpcc_by_last_name_percent);
DECLARE_double(tpcc_remote_payment_percent);

/*
 * General Command Line Arguments
 */
DECLARE_uint32(seed);
DECLARE_string(task);
DECLARE_uint32(load_parallelism);
DECLARE_uint64(size_per_thread);
DECLARE_string(benchmark);
DECLARE_string(tag);
DECLARE_string(input_folder);
DECLARE_uint32(threads);
DECLARE_bool(pin_threads);
DECLARE_bool(hyperthreading);
DECLARE_bool(abort_buffer);
DECLARE_uint32(abort_buffer_size);
DECLARE_uint32(abort_penalty);

/*
 * Partitioner/Execution Command Line Arguments
 */
DECLARE_string(objtype);
DECLARE_string(partitioner);
DECLARE_string(output_folder);
DECLARE_uint32(ufactor);

#endif //SYSTEM_PARSER_H__
