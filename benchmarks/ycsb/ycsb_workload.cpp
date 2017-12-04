#include "mem_alloc.h"
#include "ycsb.h"
#include "query.h"
#include "graph_partitioner.h"

uint64_t 	YCSBWorkloadGenerator::the_n = 0;
double 		YCSBWorkloadGenerator::zeta_n_theta = 0;
double 		YCSBWorkloadGenerator::zeta_2_theta = 0;
drand48_data * * YCSBWorkloadGenerator::buffers = NULL;


void YCSBWorkloadGenerator::initialize(uint32_t num_threads,
																			 uint64_t num_params_per_thread,
																			 const char * base_file_name) {
	ParallelWorkloadGenerator::initialize(num_threads, num_params_per_thread, base_file_name);
	YCSBWorkloadGenerator::initialize_zipf_distribution(_num_threads);
	_queries = new ycsb_query * [_num_threads];
	for(uint32_t i = 0; i < _num_threads; i++) {
		_queries[i] = new ycsb_query[_num_params_per_thread];
	}
}

void YCSBWorkloadGenerator::initialize_zipf_distribution(uint32_t num_threads) {
	assert(the_n == 0);
	uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;

	the_n 		 = table_size - 1;
	zeta_2_theta = zeta(2, g_zipf_theta);
	zeta_n_theta = zeta(the_n, g_zipf_theta);
	buffers = new drand48_data * [num_threads];
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

void YCSBWorkloadGenerator::gen_requests(uint32_t thread_id, ycsb_query * query) {
	assert(g_virtual_part_cnt == 1 && g_part_cnt == 1);
	int access_cnt = 0;
	set<uint64_t> all_keys;

	double r = 0;
	int64_t rint64 = 0;
	drand48_r(buffers[thread_id], &r);
	lrand48_r(buffers[thread_id], &rint64);

	uint64_t rid = 0;
	for (UInt32 tmp = 0; tmp < MAX_REQ_PER_QUERY; tmp ++) {
		assert(rid < MAX_REQ_PER_QUERY);

		ycsb_request * req = & (query->params.requests[rid]);

		//Choose the access type
		drand48_r(buffers[thread_id], &r);
		if (r < g_read_perc) {
			req->rtype = RD;
		} else {
			req->rtype = WR;
		}

		//Generate a key from the zipfian distribution
		uint64_t row_id = zipf(thread_id, g_synth_table_size - 1, g_zipf_theta);
		assert(row_id < g_synth_table_size);
		uint64_t primary_key = row_id;
		req->key = primary_key;

		//Generate random value for value
		lrand48_r(buffers[thread_id], &rint64);
		req->value = static_cast<char>(rint64 % (1 << 8));


		// Make sure a single row is not accessed twice
		if (req->rtype == RD || req->rtype == WR) {
			if (all_keys.find(req->key) == all_keys.end()) {
				all_keys.insert(req->key);
				access_cnt ++;
			} else {
				continue;
			}
		}
		assert(rid <= MAX_REQ_PER_QUERY);
		rid++;
	}
	query->params.request_cnt = rid;


	// Sort the requests in key order, if needed
	if (g_key_order) {
		for (int i = static_cast<int>(query->params.request_cnt - 1); i > 0; i--)
			for (int j = 0; j < i; j ++)
				if (query->params.requests[j].key > query->params.requests[j + 1].key) {
					ycsb_request tmp = query->params.requests[j];
					query->params.requests[j] = query->params.requests[j + 1];
					query->params.requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < query->params.request_cnt - 1; i++)
			assert(query->params.requests[i].key < query->params.requests[i + 1].key);
	}
	assert(query->params.request_cnt <= MAX_REQ_PER_QUERY);
}

BaseQueryList * YCSBWorkloadGenerator::get_queries_list(uint32_t thread_id) {
  auto queryList = new QueryList<ycsb_params>();
  queryList->initialize(_queries[thread_id], _num_params_per_thread);
  return queryList;
}

BaseQueryMatrix *YCSBWorkloadGenerator::get_queries_matrix() {
	auto matrix = new QueryMatrix<ycsb_params>();
	matrix->initialize(_queries, _num_threads, _num_params_per_thread);
	return matrix;
}

void YCSBWorkloadGenerator::per_thread_generate(uint32_t thread_id) {
  buffers[thread_id] =(drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
  srand48_r(thread_id + 1, buffers[thread_id]);
	for(uint64_t i = 0; i < _num_params_per_thread; i++) {
		gen_requests(thread_id, & (_queries[thread_id][i]));
	}
}

void YCSBWorkloadGenerator::per_thread_write_to_file(uint32_t thread_id, FILE *file) {
	ycsb_query * thread_queries = _queries[thread_id];
	fwrite(thread_queries, sizeof(ycsb_query), _num_params_per_thread, file);
}

BaseQueryList *YCSBWorkloadLoader::get_queries_list(uint32_t thread_id) {
	auto queryList = new QueryList<ycsb_params>();
	queryList->initialize(_queries[thread_id], _array_sizes[thread_id]);
	return queryList;
}

void YCSBWorkloadLoader::per_thread_load(uint32_t thread_id, FILE *file) {
	fseek(file, 0, SEEK_END);
	size_t bytes_to_read = ftell(file);
	fseek(file, 0, SEEK_SET);

	_array_sizes[thread_id] = bytes_to_read / sizeof(ycsb_query);
	_queries[thread_id] 		= (ycsb_query *) _mm_malloc(bytes_to_read, 64);

	size_t bytes_read = fread(_queries[thread_id], sizeof(ycsb_query), _array_sizes[thread_id], file);
	assert(bytes_read == bytes_to_read);
}

void YCSBWorkloadLoader::initialize(uint32_t num_threads, char *base_file_name) {
	ParallelWorkloadLoader::initialize(num_threads, base_file_name);
	_queries = new ycsb_query * [_num_threads];
	_array_sizes = new uint32_t[_num_threads];
}

BaseQueryMatrix * YCSBWorkloadLoader::get_queries_matrix() {
	uint32_t const_size = _array_sizes[0];
	for(uint32_t i = 0; i < _num_threads; i++) {
		assert(_array_sizes[i] == const_size);
	}

	auto qm = new QueryMatrix<ycsb_params>();
	qm->initialize(_queries, _num_threads, const_size);
	return qm;
}



void YCSBWorkloadPartitioner::initialize(BaseQueryMatrix * queries,
																				 uint64_t max_cluster_graph_size,
																				 uint32_t parallelism) {
	ParallelWorkloadPartitioner::initialize(queries, max_cluster_graph_size, parallelism);
	_partitioned_queries = nullptr;
}

BaseQueryList *YCSBWorkloadPartitioner::get_queries_list(uint32_t thread_id) {
	assert(_partitioned_queries != nullptr);
	auto queryList = new QueryList<ycsb_params>();
	queryList->initialize(_partitioned_queries[thread_id], _tmp_queries[thread_id].size());
	return queryList;
}

int YCSBWorkloadPartitioner::compute_weight(BaseQuery * q1, BaseQuery * q2) {
	assert(q1 != q2);
	bool conflict = false;
	int weight = 0;
	ycsb_params * p1 = & ((ycsb_query *)q1)->params;
	ycsb_params * p2 = & ((ycsb_query *)q2)->params;
	for(uint32_t i = 0; i < p1->request_cnt; i++) {
		for(uint32_t j = 0; j < p2->request_cnt; j++) {
		  if((p1->requests[i].key == p2->requests[j].key) && (p1->requests[i].rtype == WR || p2->requests[i].rtype == WR)) {
				weight += 1;
				conflict = true;
				break;
			}
		}
	}
	return conflict ? weight : -1;
}

void YCSBWorkloadPartitioner::partition() {
	ParallelWorkloadPartitioner::partition();

	uint64_t start_time, end_time;
	start_time = get_server_clock();
	_partitioned_queries = new ycsb_query * [_num_arrays];
	for(uint32_t i = 0; i < _num_arrays; i++) {
		_partitioned_queries[i] = (ycsb_query *) _mm_malloc(sizeof(ycsb_query) * _tmp_queries[i].size(), 64);
		uint32_t offset = 0;
		for(auto iter = _tmp_queries[i].begin(); iter != _tmp_queries[i].end(); iter++) {
			memcpy(& _partitioned_queries[i][offset], * iter, sizeof(ycsb_query));
			offset++;
		}
	}
	end_time = get_server_clock();
	shuffle_duration += DURATION(end_time, start_time);
}

void YCSBWorkloadPartitioner::write_workload_file(uint32_t thread_id, FILE *file) {
	ycsb_query * thread_queries = _partitioned_queries[thread_id];
	uint32_t size = _tmp_array_sizes[thread_id];
	fwrite(thread_queries, sizeof(ycsb_query), size, file);
}


void YCSBExecutor::initialize(uint32_t num_threads, const char * path) {
	BenchmarkExecutor::initialize(num_threads, path);

	//Build database in parallel
	_db = new YCSBDatabase();
	_db->initialize(INIT_PARALLELISM);
	_db->load();

	//Load workload in parallel
	_loader = new YCSBWorkloadLoader();
	_loader->initialize(num_threads, _path);
	_loader->load();

	//Initialize each thread
	for(uint32_t i = 0; i < _num_threads; i++) {
		_threads[i].initialize(i, _db, _loader->get_queries_list(i), true);
	}
}