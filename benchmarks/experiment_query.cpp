#include "query.h"
#include "experiment_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "experiment.h"
#include "table.h"

uint64_t experiment_query::the_n = 0;
double experiment_query::denom = 0;

void experiment_query::init(uint64_t thd_id, workload * h_wl, Query_thd * query_thd) 
{
	_query_thd = query_thd;
	requests = (experiment_request *) mem_allocator.alloc(sizeof(experiment_request) * g_req_per_query, thd_id);
	part_to_access = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thd_id);
	zeta_2_theta = zeta(2, g_zipf_theta);
	assert(the_n != 0);
	assert(denom != 0);
	gen_requests(thd_id, h_wl);
}

void experiment_query::calculateDenom()
{
	assert(the_n == 0);
	uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
	the_n = table_size - 1;
	denom = zeta(the_n, g_zipf_theta);
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug. 
// The original paper says zeta(theta, 2.0). But I guess it should be 
// zeta(2.0, theta).
double experiment_query::zeta(uint64_t n, double theta) 
{
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) {
		sum += pow(1.0 / i, theta);
	}
	return sum;
}

uint64_t experiment_query::zipf(uint64_t n, double theta) 
{
	assert(this->the_n == n);
	assert(theta == g_zipf_theta);
	double alpha = 1 / (1 - theta);
	double zetan = denom;
	double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
	double u; 
	drand48_r(&_query_thd->buffer, &u);
	double uz = u * zetan;
	if (uz < 1) return 1;
	if (uz < 1 + pow(0.5, theta)) return 2;
	return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}

void experiment_query::gen_requests(uint64_t thd_id, workload * h_wl) 
{
	#if CC_ALG == HSTORE
		assert(g_virtual_part_cnt == g_part_cnt);
	#endif

	int access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;

	UInt32 rid = 0;
	for (UInt32 tmp = 0; tmp < g_req_per_query; tmp++) {			
		double r;
		drand48_r(&_query_thd->buffer, &r);
		experiment_request * req = &requests[rid];
		if (r < g_read_perc) {
			req->rtype = RD;
		} else if (r >= g_read_perc && r < g_write_perc + g_read_perc) {
			req->rtype = WR;
		} else {
			req->rtype = SCAN;
			req->scan_len = SCAN_LEN;
		}

		//get appropriate key
		uint64_t primary_key;
		int64_t rint64;
		drand48_r(&_query_thd->buffer, &r);
		if((rid + 1 == g_pos_in_txn) && (r < g_contention_perc)) {
			primary_key = 42;
			req->rtype = WR;
		} else {
			lrand48_r(&_query_thd->buffer, &rint64);
			primary_key = rint64 % g_synth_table_size;	
			assert(primary_key >= 0 && primary_key < g_synth_table_size);
		}
		req->key = primary_key;

		lrand48_r(&_query_thd->buffer, &rint64);
		req->value = rint64 % (1 << 8);

		// Make sure a single row is not accessed twice
		if (req->rtype == RD || req->rtype == WR) {
			if (all_keys.find(req->key) == all_keys.end()) {
				all_keys.insert(req->key);
				access_cnt++;
			} else {
				continue;
			}
		} else {
			bool conflict = false;
			for (UInt32 i = 0; i < req->scan_len; i++) {
				if (all_keys.find(req->key) != all_keys.end())
					conflict = true;
			}
			
			if (conflict) {
				continue;
			} else {
				for (UInt32 i = 0; i < req->scan_len; i++) {
					all_keys.insert(req->key + i);
				}
				access_cnt += SCAN_LEN;
			}
		}
		rid ++;
	}
	request_cnt = rid;

	// Sort the requests in key order.
	if (g_key_order) {
		for (int i = request_cnt - 1; i > 0; i--) 
			for (int j = 0; j < i; j ++)
				if (requests[j].key > requests[j + 1].key) {
					experiment_request tmp = requests[j];
					requests[j] = requests[j + 1];
					requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < request_cnt - 1; i++)
			assert(requests[i].key < requests[i + 1].key);
	}

}


