#ifndef _YCSB_QUERY_H_
#define _YCSB_QUERY_H_

#include "Global.h"
#include "Helper.h"
#include "Query.h"

class Workload;
class QueryQueue;
// Each ycsb_query contains several ycsb_requests, 
// each of which is a RD, WR to a single table

class ycsb_request {
public:
	AccessType rtype; 
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

class YCSB_Query : public Query {
public:
	void initialize(uint64_t thd_id, Workload * h_wl) { assert(false); };
	void init(uint64_t thd_id, Workload * h_wl, QueryQueue * query_thd);
	static void calculateDenom();

	uint64_t request_cnt;
	ycsb_request * requests;

private:
	void gen_requests(uint64_t thd_id, Workload * h_wl);
	// for Zipfian distribution
	static double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;
	QueryQueue * _query_thd;
};

#endif
