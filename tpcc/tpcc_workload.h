// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_WORKLOAD_GENERATOR_H_
#define TPCC_TPCC_WORKLOAD_GENERATOR_H_

#include "generator.h"
#include "global.h"
#include "loader.h"
#include "offline_scheduler.h"
#include "tpcc_database.h"
#include "tpcc_utility.h"
#include "scheduler.h"

class TPCCWorkloadGenerator : public ParallelWorkloadGenerator<tpcc_params> {
  public:
    TPCCWorkloadGenerator(const TPCCBenchmarkConfig &_config, uint64_t num_threads,
                          uint64_t size_per_thread, const string &base_file_name);

  protected:
    void per_thread_generate(uint64_t thread_id) override;
    void gen_payment_request(uint64_t thread_id, tpcc_query *params);
    void gen_new_order_request(uint64_t thread_id, tpcc_query *params);

    TPCCBenchmarkConfig config;
    TPCCHelper helper;
    RandomNumberGenerator _random;
    uint64_t *num_orders;
    uint64_t *num_order_txns;
};

typedef ParallelWorkloadLoader<tpcc_params> TPCCWorkloadLoader;

class TPCCExecutor {
  public:
    TPCCExecutor(const TPCCBenchmarkConfig &config, const string &folder_path,
                 uint64_t num_threads);

	void execute();
	void release();
  protected:
    TPCCDatabase _db;
    TPCCWorkloadLoader _loader;
    OnlineScheduler<tpcc_params> *_scheduler;
};

#endif // TPCC_TPCC_WORKLOAD_GENERATOR_H_
