// Copyright[2017] <Guna Prasaad>

#ifndef YCSB_YCSB_WORKLOAD_H_
#define YCSB_YCSB_WORKLOAD_H_

#include "distributions.h"
#include "generator.h"
#include "loader.h"
#include "thread.h"
#include "scheduler.h"
#include "ycsb_database.h"
#include "online_batch_scheduler.h"

class YCSBWorkloadGenerator : public ParallelWorkloadGenerator<ycsb_params> {
public:
  YCSBWorkloadGenerator(const YCSBBenchmarkConfig &_config,
                        uint64_t num_threads, uint64_t size_per_thread,
                        const string &folder_path);

protected:
  void per_thread_generate(uint64_t thd_id) override;
  void gen_single_partition_requests(uint64_t thd_id, ycsb_query *query);
  void gen_multi_partition_requests(uint64_t thd_id, ycsb_query *query);

  YCSBBenchmarkConfig config;
  ZipfianNumberGenerator zipfian;
  RandomNumberGenerator _random;
};

typedef ParallelWorkloadLoader<ycsb_params> YCSBWorkloadLoader;


class YCSBExecutor {
public:
	YCSBExecutor(const YCSBBenchmarkConfig &_config, const string &folder_path,
	             uint64_t num_threads);
	void execute();
	void release();
protected:
	YCSBDatabase _db;
	YCSBWorkloadLoader _loader;
	IScheduler<ycsb_params>* _scheduler;
};



#endif // YCSB_YCSB_WORKLOAD_H_
