// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_WORKLOAD_GENERATOR_H_
#define TPCC_TPCC_WORKLOAD_GENERATOR_H_

#include "generator.h"
#include "global.h"
#include "tpcc_database.h"
#include "tpcc_helper.h"

class TPCCWorkloadGenerator : public ParallelWorkloadGenerator {
public:
		TPCCWorkloadGenerator(uint32_t num_threads, uint64_t num_params_per_thread,
													const string &base_file_name);
		BaseQueryList *get_queries_list(uint32_t thread_id) override;
		BaseQueryMatrix *get_queries_matrix() override;
protected:
		void per_thread_generate(uint32_t thread_id) override;
		void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;
		void gen_payment_request(uint64_t thread_id, tpcc_payment_params *params);
		void gen_new_order_request(uint64_t thd_id, tpcc_new_order_params *params);
		tpcc_query **_queries;
};

class TPCCWorkloadLoader : public ParallelWorkloadLoader {
public:
		void initialize(uint32_t num_threads, const char *base_file_name) override;
		BaseQueryList *get_queries_list(uint32_t thread_id) override;
		BaseQueryMatrix *get_queries_matrix() override;

protected:
		void per_thread_load(uint32_t thread_id, FILE *file) override;
		tpcc_query **_queries;
		uint32_t *_array_sizes;
};

class TPCCExecutor : public BenchmarkExecutor {
public:
		void initialize(uint32_t num_threads, const char *path) override;

protected:
		TPCCDatabase *_db;
		TPCCWorkloadLoader *_loader;
};

#endif //TPCC_TPCC_WORKLOAD_GENERATOR_H_
