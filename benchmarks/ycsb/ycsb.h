#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "global.h"
#include "helper.h"
#include "workload.h"
#include "database.h"
#include "txn.h"
#include "query.h"
#include "thread.h"
#include <vector>

class ycsb_request {
public:
	access_t rtype;
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

struct ycsb_params {
  uint64_t 	request_cnt;
  ycsb_request 	requests[MAX_REQ_PER_QUERY];
};

typedef Query<ycsb_params> ycsb_query;

class YCSBDatabase;

class YCSBTransactionManager : public txn_man {
public:
	void initialize(Database * database, uint32_t thread_id) override;
	RC run_txn(BaseQuery * query) override;

	uint64_t 		row_cnt;
	YCSBDatabase * 	db;
};

class YCSBDatabase : public Database {
public :
	void        initialize(uint32_t num_threads) override;
	txn_man *   get_txn_man(uint32_t thread_id) override;
	int         key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;

protected:
	void        load_tables(uint32_t thread_id) override;
	void        load_main_table(uint32_t thread_id);
};

class YCSBWorkloadGenerator : public ParallelWorkloadGenerator {
public:
	void initialize(uint32_t num_threads,
					uint64_t num_params_per_thread,
					const char * base_file_name) override;

	BaseQueryList * 		get_queries_list(uint32_t thread_id) override;
	BaseQueryMatrix *   get_queries_matrix() override;
protected:
	void            per_thread_generate(uint32_t thread_id) override;
	void            per_thread_write_to_file(uint32_t thread_id, FILE * file) override;
	void 						gen_requests(uint64_t thd_id, ycsb_query * query);

	ycsb_query * * 		_queries;

	static 	void 		initialize_zipf_distribution(uint32_t num_threads);
	static 	double 		zeta(uint64_t n, double theta);
	static 	uint64_t 	zipf(uint32_t thread_id, uint64_t n, double theta);

	static drand48_data * *		        buffers;
	static 	double 				zeta_n_theta;
	static 	double 				zeta_2_theta;
	static 	uint64_t 			the_n;

	friend class YCSBWorkloadPartitioner;
};


class YCSBWorkloadPartitioner : public WorkloadPartitioner {
public:
	void  					initialize				(uint32_t num_threads,
																		 uint64_t num_params_per_thread,
																		 uint64_t num_params_pgpt,
																		 ParallelWorkloadGenerator * generator) override;
	void 						partition					() override;
	BaseQueryList * get_queries_list	(uint32_t thread_id) override;
protected:
	int 						compute_weight		(BaseQuery * q1, BaseQuery * q2) override;

	ycsb_query * *  _partitioned_queries;
};


class YCSBExecutor : public BenchmarkExecutor {
public:
    void initialize(uint32_t num_threads) override;
protected:
    YCSBDatabase * 					_db;
    YCSBWorkloadGenerator * _generator;
};

#endif