#include "mem_alloc.h"
#include "ycsb.h"

uint64_t 	YCSBWorkloadGenerator::the_n = 0;
double 		YCSBWorkloadGenerator::zeta_n_theta = 0;
double 		YCSBWorkloadGenerator::zeta_2_theta = 0;
drand48_data * * YCSBWorkloadGenerator::buffers = NULL;

void YCSBWorkloadGenerator::initialize_zipf_distribution(uint32_t num_threads) {
	assert(the_n == 0);
	uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;

	the_n 		 = table_size - 1;
	zeta_2_theta = zeta(2, g_zipf_theta);
	zeta_n_theta = zeta(the_n, g_zipf_theta);
	buffers = new drand48_data * [num_threads];
	for(uint32_t i = 0; i < num_threads; i++) {
		buffers[i] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	}
}

double YCSBWorkloadGenerator::zeta(uint64_t n, double theta) {
	// The following algorithm comes from the paper:
	// Quickly generating billion-record synthetic databases
	// However, it seems there is a small bug.
	// The original paper says zeta(theta, 2.0). But I guess it should be
	// zeta(2.0, theta).
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) {
		sum += pow(1.0 / i, theta);
	}
	return sum;
}

uint64_t YCSBWorkloadGenerator::zipf(uint32_t thread_id, uint64_t n, double theta) {
	assert(the_n == n);
	assert(theta == g_zipf_theta);
	double alpha = 1 / (1 - theta);
	double zetan = zeta_n_theta;
	double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
	double u; 
	drand48_r(buffers[thread_id], &u);
	double uz = u * zetan;
	if (uz < 1) return 1;
	if (uz < 1 + pow(0.5, theta)) return 2;
	return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}

void YCSBWorkloadGenerator::gen_requests(uint64_t thread_id, ycsb_query * query) {
	assert(g_virtual_part_cnt == 1 && g_part_cnt == 1);
	int access_cnt = 0;
	set<uint64_t> all_keys;

	double r = 0;
	int64_t rint64 = 0;
	drand48_r(buffers[thread_id], &r);
	lrand48_r(buffers[thread_id], &rint64);

	int rid = 0;
	for (UInt32 tmp = 0; tmp < g_req_per_query; tmp ++) {		
		
		double r;
		drand48_r(buffers[thread_id], &r);
		ycsb_request * req = & query->params.requests[rid];
		if (r < g_read_perc) {
			req->rtype = RD;
		} else if (r >= g_read_perc && r < g_write_perc + g_read_perc) {
			req->rtype = WR;
		} else {
			req->rtype = SCAN;
			req->scan_len = SCAN_LEN;
		}

		// the request will access part_id.
		uint32_t part_id = 0;
		uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
		uint64_t row_id = zipf(thread_id, table_size - 1, g_zipf_theta);
		assert(row_id < table_size);
		uint64_t primary_key = row_id * g_virtual_part_cnt + part_id;
		req->key = primary_key;
		int64_t rint64;
		lrand48_r(buffers[thread_id], &rint64);
		req->value = rint64 % (1<<8);
		// Make sure a single row is not accessed twice
		if (req->rtype == RD || req->rtype == WR) {
			if (all_keys.find(req->key) == all_keys.end()) {
				all_keys.insert(req->key);
				access_cnt ++;
			} else {
				continue;
			}
		} else {
			bool conflict = false;
			for (UInt32 i = 0; i < req->scan_len; i++) {
				primary_key = (row_id + i) * g_part_cnt + part_id;
				if (all_keys.find( primary_key )
					!= all_keys.end())
					conflict = true;
			}
			if (conflict) {
				continue;
			} else {
				for (UInt32 i = 0; i < req->scan_len; i++) {
					all_keys.insert( (row_id + i) * g_part_cnt + part_id);
				}
				access_cnt += SCAN_LEN;
			}
		}
		rid ++;
	}
	query->params.request_cnt = rid;

	// Sort the requests in key order.
	if (g_key_order) {
		for (int i = query->params.request_cnt - 1; i > 0; i--)
			for (int j = 0; j < i; j ++)
				if (query->params.requests[j].key > query->params.requests[j + 1].key) {
					ycsb_request tmp = query->params.requests[j];
					query->params.requests[j] = query->params.requests[j + 1];
					query->params.requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < query->params.request_cnt - 1; i++)
			assert(query->params.requests[i].key < query->params.requests[i + 1].key);
	}

}

void YCSBWorkloadGenerator::initialize(uint32_t num_threads, uint64_t num_params, char *base_file_name) {
    WorkloadGenerator::initialize(num_threads, num_params, base_file_name);
    YCSBWorkloadGenerator::initialize_zipf_distribution(_num_threads);
    _queries = new ycsb_query * [_num_threads];
    for(uint32_t i = 0; i < _num_threads; i++) {
      _queries[i] = new ycsb_query[_num_params_per_thread];
    }
}

BaseQueryList * YCSBWorkloadGenerator::get_queries_list(uint32_t thread_id) {
  auto queryList = new QueryList<ycsb_params>();
  queryList->initialize(_queries[thread_id], _num_params_per_thread);
  return queryList;
}

void YCSBWorkloadGenerator::per_thread_generate(uint32_t thread_id) {
	ycsb_query * thread_queries = _queries[thread_id];
	for(uint64_t i = 0; i < _num_params_per_thread; i++) {
		gen_requests(thread_id, & (thread_queries[i]));
	}
}

void YCSBWorkloadGenerator::per_thread_write_to_file(uint32_t thread_id, FILE *file) {
	ycsb_query * thread_queries = _queries[thread_id];
	fwrite(thread_queries, sizeof(ycsb_query), _num_params_per_thread, file);
}


