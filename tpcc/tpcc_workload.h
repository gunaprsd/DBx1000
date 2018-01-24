// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_WORKLOAD_GENERATOR_H_
#define TPCC_TPCC_WORKLOAD_GENERATOR_H_

#include "generator.h"
#include "global.h"
#include "loader.h"
#include "tpcc_database.h"
#include "tpcc_utility.h"

class TPCCWorkloadGenerator : public ParallelWorkloadGenerator<tpcc_params> {
public:
  TPCCWorkloadGenerator(const TPCCBenchmarkConfig &_config,
                        uint64_t num_threads, uint64_t size_per_thread,
                        const string &base_file_name);

protected:
  void per_thread_generate(uint64_t thread_id) override;
  void gen_payment_request(uint64_t thread_id, tpcc_payment_params *params);
  void gen_new_order_request(uint64_t thread_id, tpcc_new_order_params *params);

  TPCCBenchmarkConfig config;
  TPCCHelper helper;
  RandomNumberGenerator _random;
};

typedef ParallelWorkloadLoader<tpcc_params> TPCCWorkloadLoader;

class TPCCExecutor : public BenchmarkExecutor<tpcc_params> {
public:
  TPCCExecutor(const TPCCBenchmarkConfig &config, const string &folder_path,
               uint64_t num_threads);

protected:
  TPCCDatabase _db;
  TPCCWorkloadLoader _loader;
};

#endif // TPCC_TPCC_WORKLOAD_GENERATOR_H_
