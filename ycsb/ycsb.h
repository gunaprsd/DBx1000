// Copyright[2017] <Guna Prasaad>

#ifndef YCSB_YCSB_H_
#define YCSB_YCSB_H_


#include "query.h"

class ycsb_request {
public:
  access_t rtype;
  uint64_t key;
  char value;
  uint32_t scan_len;
};

struct ycsb_params {
  uint64_t request_cnt;
  ycsb_request requests[YCSB_NUM_REQUESTS];
};

typedef Query<ycsb_params> ycsb_query;

struct YCSBBenchmarkConfig {
		// the table is expected to contain 0 -> (table_size - 1) keys
		uint64_t table_size;

		// zipfian parameter to generate the workload
		double zipfian_theta;

		//  percent of read accesses (normalized to 1.0)
		double read_percent;

		// total number of partitions
		uint32_t num_partitions;

		// percent of multi-partition transactions (MPT)
		double multi_part_txns_percent;

		// number of partitions accessed by an MPT
		uint32_t num_local_partitions;

		// number of accesses that are remote in MPT
		double remote_access_percent;

		//number of remote partitions for each set of local partitions in MPT
		uint32_t num_remote_partitions;

		// ordered key access
		bool key_order;

		// perform compute for every key
		bool do_compute;

		// amount of compute cost performed for each key
		uint64_t compute_cost;
};

class YCSBUtility {
public:
		static void initialize(YCSBBenchmarkConfig & config);
		static uint64_t get_max_key() {
			return max_key;
		}
		static uint64_t get_hash(uint64_t key) {
			return key % max_key;
		}
protected:
		static uint64_t max_key;
};


#endif // YCSB_YCSB_H_
