#include "tpcc.h"


BaseQueryList *TPCCConflictGraphPartitioner::get_queries_list(uint32_t thread_id) {
	auto queryList = new QueryList<tpcc_params>();
	queryList->initialize(_partitioned_queries[thread_id], _tmp_queries[thread_id].size());
	return queryList;
}

void TPCCConflictGraphPartitioner::initialize(BaseQueryMatrix * queries,
																							uint64_t max_cluster_graph_size,
																							uint32_t parallelism,
																							const char * dest_folder_path) {
	ConflictGraphPartitioner::initialize(queries, max_cluster_graph_size, parallelism, dest_folder_path);
	_partitioned_queries = nullptr;
}

void TPCCConflictGraphPartitioner::partition() {
	ConflictGraphPartitioner::partition();

	uint64_t start_time, end_time;
	start_time = get_server_clock();
	_partitioned_queries = new tpcc_query * [_num_arrays];
	for(uint32_t i = 0; i < _num_arrays; i++) {
		_partitioned_queries[i] = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * _tmp_queries[i].size(), 64);
		uint32_t offset = 0;
		for(auto iter = _tmp_queries[i].begin(); iter != _tmp_queries[i].end(); iter++) {
			memcpy(& _partitioned_queries[i][offset], * iter, sizeof(tpcc_query));
			offset++;
		}
	}
	end_time = get_server_clock();
	shuffle_duration += DURATION(end_time, start_time);
}

void TPCCConflictGraphPartitioner::per_thread_write_to_file(uint32_t thread_id, FILE *file) {
	tpcc_query * thread_queries = _partitioned_queries[thread_id];
	uint32_t size = _tmp_array_sizes[thread_id];
	fwrite(thread_queries, sizeof(tpcc_query), size, file);
}

void
TPCCAccessGraphPartitioner::initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size, uint32_t parallelism,
																			 const char *dest_folder_path) {
	AccessGraphPartitioner::initialize(queries, max_cluster_graph_size, parallelism, dest_folder_path);
	assert(false);
}

void TPCCAccessGraphPartitioner::partition() {
	AccessGraphPartitioner::partition();
	assert(false);
}

void TPCCAccessGraphPartitioner::first_pass() {
	assert(false);
}

void TPCCAccessGraphPartitioner::second_pass() {
	assert(false);
}

void TPCCAccessGraphPartitioner::third_pass() {
	assert(false);
}

void TPCCAccessGraphPartitioner::compute_post_stats(idx_t *parts) {
	assert(false);
}

void TPCCAccessGraphPartitioner::partition_per_iteration() {
	assert(false);
}

void TPCCAccessGraphPartitioner::per_thread_write_to_file(uint32_t thread_id, FILE *file) {
	assert(false);
}
