#ifndef _EXPERIMENT_QUERY_H_
#define _EXPERIMENT_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class Workload;
class Query_thd;
// Each experiment_query contains several experiment_requests, 
// each of which is a RD, WR to a single table

class experiment_request {
public:
	access_t rtype; 
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

class experiment_query : public base_query {
public:
	void init(uint64_t thd_id, Workload * h_wl) { assert(false); };
	void init(uint64_t thd_id, Workload * h_wl, Query_thd * query_thd);
	static void calculateDenom();

	uint64_t request_cnt;
	experiment_request * requests;

private:
	void gen_requests(uint64_t thd_id, Workload * h_wl);
	// for Zipfian distribution
	static double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;
	Query_thd * _query_thd;
};

#endif
