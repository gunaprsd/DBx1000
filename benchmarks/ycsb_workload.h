#ifndef _YCSB_QUERY_H_
#define _YCSB_QUERY_H_

#include "workload.h"
#include "global.h"
#include "helper.h"
#include "query.h"
#include "ycsb.h"

class YCSBWorkloadGenerator : public WorkloadGenerator {
public:
	void initialize(uint32_t num_threads, uint64_t num_params, char * base_file_name) override;
	BaseQuery *     get_queries(uint32_t thread_id) override;
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



#endif
