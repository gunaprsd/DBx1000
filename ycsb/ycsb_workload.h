// Copyright[2017] <Guna Prasaad>

#ifndef YCSB_YCSB_WORKLOAD_H_
#define YCSB_YCSB_WORKLOAD_H_

#include "distributions.h"
#include "generator.h"
#include "loader.h"
#include "thread.h"
#include "ycsb_database.h"

struct YCSBWorkloadConfig {
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

	double remote_access_percent;

	uint32_t num_remote_partitions;
};

class YCSBWorkloadGenerator : public ParallelWorkloadGenerator<ycsb_params> {
public:
  YCSBWorkloadGenerator(const YCSBWorkloadConfig &config,
												uint32_t num_threads,
                        uint64_t num_queries_per_thread,
                        const string &folder_path);
protected:
  void per_thread_generate(uint32_t thread_id) override;
  void gen_single_partition_requests(uint32_t thd_id, ycsb_query *query);
	void gen_multi_partition_requests(uint32_t thd_id, ycsb_query *query);

  YCSBWorkloadConfig _config;
  ZipfianNumberGenerator _zipfian;
  RandomNumberGenerator _random;
};

typedef ParallelWorkloadLoader<ycsb_params> YCSBWorkloadLoader;

class YCSBExecutor : public BenchmarkExecutor<ycsb_params> {
public:
		void initialize(const string & folder_path, uint32_t num_threads) override;
protected:
		YCSBDatabase * _db;
		YCSBWorkloadLoader * _loader;
};

#endif // YCSB_YCSB_WORKLOAD_H_
