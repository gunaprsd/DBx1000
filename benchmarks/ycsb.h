#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "global.h"
#include "helper.h"
#include "workload.h"
#include "database.h"
#include "txn.h"
#include "query.h"
#include "thread.h"
#include "conflict_graph.h"

class ycsb_request {
public:
	access_t rtype;
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

struct ycsb_params {
	uint64_t 		request_cnt;
	ycsb_request 	requests[MAX_REQ_PER_QUERY];
};

typedef Query<ycsb_params> ycsb_query;

class YCSBDatabase;

class YCSBTransactionManager : public txn_man {
public:
	void initialize(Database * database, INDEX * index, uint32_t thread_id);
	RC run_txn(BaseQuery * query) override;

	uint64_t 		row_cnt;
	YCSBDatabase * 	ycsb_database;
	INDEX *			the_index;
};

class YCSBWorkloadGenerator : public WorkloadGenerator {
public:
	void initialize(uint32_t num_threads,
		   			uint64_t num_params_per_thread,
		   			const char * base_file_name) override;

	BaseQueryList * get_queries_list(uint32_t thread_id) override;
protected:
	void            per_thread_generate(uint32_t thread_id) override;
	void            per_thread_write_to_file(uint32_t thread_id, FILE * file) override;
	void 			gen_requests(uint64_t thd_id, ycsb_query * query);

	ycsb_query * * 		_queries;

	static 	void 		initialize_zipf_distribution(uint32_t num_threads);
	static 	double 		zeta(uint64_t n, double theta);
	static 	uint64_t 	zipf(uint32_t thread_id, uint64_t n, double theta);

	static drand48_data * *		buffers;
	static 	double 				zeta_n_theta;
	static 	double 				zeta_2_theta;
	static 	uint64_t 			the_n;
};

class YCSBDatabase : public Database {
public :
	void        initialize(uint32_t num_threads = INIT_PARALLELISM) override;
	txn_man *   get_txn_man(uint32_t thread_id) override;
	int         key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;

protected:
	void        load_tables(uint32_t thread_id) override;
	void        load_main_table(uint32_t thread_id);
};

class YCSBExecutor : public BenchmarkExecutor {
public:
	void initialize(uint32_t num_threads) override;
protected:
	YCSBDatabase * 			_db;
	YCSBWorkloadGenerator * _generator;
};

class YCSBGraphGenerator : public ExtrernalConflictGraphGenerator {
	struct DataInfo {
		uint64_t num_reads;
		uint64_t num_writes;
	};

protected:
	void create_conflict_graph(uint64_t start_offset, uint64_t num_records, FILE * out_file) override;

private:
	uint64_t hash(uint64_t key) {
		return key;
	}

	double compute_weight(ycsb_query * q1, ycsb_query * q2, DataInfo * data) {
		bool conflict = false;
		double weight = 0.0;
		ycsb_params * p1 = & q1->params;
		ycsb_params * p2 = & q2->params;
		for(uint32_t i = 0; i < p1->request_cnt; i++) {
			for(uint32_t j = 0; j < p2->request_cnt; j++) {
				if(p1->requests[i].key == p2->requests[j].key) {
					weight += 1.0;
					conflict = true;
				}
			}
		}
		return conflict ? weight : -1.0;
	}
};

class YCSBWorkloadPartitioner : public WorkloadPartitioner {
    struct DataInfo {
        uint64_t num_reads;
        uint64_t num_writes;
    };

protected:
    void partition_workload_part(uint32_t iteration, uint64_t num_records) override;

private:
    uint64_t hash(uint64_t key) {
        return key;
    }

    double compute_weight(ycsb_query * q1, ycsb_query * q2, DataInfo * data) {
        bool conflict = false;
        double weight = 0.0;
        ycsb_params * p1 = & q1->params;
        ycsb_params * p2 = & q2->params;
        for(uint32_t i = 0; i < p1->request_cnt; i++) {
            for(uint32_t j = 0; j < p2->request_cnt; j++) {
                if(p1->requests[i].key == p2->requests[j].key) {
                    weight += 1.0;
                    conflict = true;
                }
            }
        }
        return conflict ? weight : -1.0;
    }

};
#endif
