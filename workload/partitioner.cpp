//
// Created by Guna Prasaad on 07/12/17.
//

#include "global.h"
#include "helper.h"
#include "query.h"
#include "partitioner.h"



void ParallelWorkloadPartitioner::initialize(BaseQueryMatrix * queries,
					     uint64_t max_cluster_graph_size,
					     uint32_t parallelism,
							 const char * dest_folder_path)
{
	strcpy(_folder_path, dest_folder_path);
	_orig_queries           = queries;
	_parallelism            = parallelism;
	_num_arrays            	= _orig_queries->num_arrays;
	_num_queries_per_array  = _orig_queries->num_queries_per_array;

	_max_cluster_graph_size = max_cluster_graph_size;
	_num_queries_per_iter_per_array = max_cluster_graph_size / _num_arrays;

	assert(_num_queries_per_array % _num_queries_per_iter_per_array == 0);
	assert(_num_queries_per_iter_per_array % _parallelism == 0);

	_tmp_queries        = new std::vector<BaseQuery*> [_num_arrays];
	_tmp_array_sizes    = new uint32_t[_num_arrays];
	for(uint32_t i = 0; i < _num_arrays; i++) {  _tmp_array_sizes[i] = 0; }

	_current_iteration          	= 0;
	_total_num_edges            	= 0;
	_total_pre_cross_core_edges 	= 0;
	_total_pre_cross_core_weight	= 0;
	_total_post_cross_core_edges	= 0;
	_total_post_cross_core_weight = 0;

	data_statistics_duration    = 0.0;
	graph_init_duration         = 0.0;
	partition_duration          = 0.0;
	shuffle_duration            = 0.0;

	_current_parts = (idx_t *) malloc(sizeof(idx_t) * _max_cluster_graph_size);
	for(uint64_t i = 0; i < _max_cluster_graph_size; i++) { _current_parts[i] = -1; }

}


void ParallelWorkloadPartitioner::partition()
{
	while(_array_iter_start_offset < _num_queries_per_array) {
		_total_num_edges                = 0;
		_total_pre_cross_core_edges     = 0;
		_total_pre_cross_core_weight    = 0;
		_total_post_cross_core_edges    = 0;
		_total_post_cross_core_weight   = 0;

		//create and write conflict graph
		partition_per_iteration();
		print_execution_summary();

		//move to next region
		_current_iteration++;
		_array_iter_start_offset = (_current_iteration * _num_queries_per_iter_per_array);
		for(uint32_t i = 0; i < _num_arrays; i++) { _tmp_array_sizes[i] = (uint32_t)_tmp_queries[i].size(); }
	}
}

void ParallelWorkloadPartitioner::write_to_files() {
	for(uint32_t i = 0; i < _num_arrays; i++) {
		char file_name[200];
		get_workload_file_name(_folder_path, i, file_name);
		FILE * file = fopen(file_name, "w");
		per_thread_write_to_file(i, file);
		fflush(file);
		fclose(file);
	}
}

void ParallelWorkloadPartitioner::partition_per_iteration()
{
	uint64_t start_time, end_time;
	double duration;
	if(WRITE_PARTITIONS_TO_FILE) {	write_pre_partition_file(); }

	start_time = get_server_clock();
	compute_data_info();
	end_time = get_server_clock();
	duration = DURATION(end_time, start_time);
	data_statistics_duration += duration;
	printf("Compute Statistics Completed in %lf secs\n", duration);


	for(uint64_t i = 0; i < _max_cluster_graph_size; i++) { _current_parts[i] = -1; }

	if(_parallelism > 1) {
		start_time = get_server_clock();
		auto graph = create_graph();
		end_time = get_server_clock();
		duration = DURATION(end_time, start_time);
		graph_init_duration += duration;
		printf("METIS_CSRGraph Init Completed in %lf secs\n", duration);

		start_time = get_server_clock();
		METISGraphPartitioner::compute_partitions(graph, _num_arrays, _current_parts);
		end_time = get_server_clock();
		duration = DURATION(end_time, start_time);
		partition_duration += duration;
		printf("METIS Partitioning Completed in %lf secs\n", duration);
	} else {

		start_time = get_server_clock();
		auto graph = parallel_create_graph();
		end_time = get_server_clock();
		duration = DURATION(end_time, start_time);
		graph_init_duration += duration;
		printf("ParMETIS_CSRGraph Init with %d threads Completed in %lf secs\n", _parallelism, duration);

		start_time = get_server_clock();
		ParMETISGraphPartitioner::compute_partitions(graph, _num_arrays, _current_parts);
		end_time = get_server_clock();
		duration = DURATION(end_time, start_time);
		partition_duration += duration;
		printf("ParMETIS Partitioning with %d threads Completed in %lf secs\n", _parallelism, duration);
	}

	//Add query pointers into tmp_queries
	BaseQuery * query = nullptr;
	int partition;
	for(uint64_t i = 0; i < _max_cluster_graph_size; i++) {
		partition = static_cast<int>(_current_parts[i]);
		assert(partition != -1);
		get_query(i, query);
		_tmp_queries[partition].push_back(query);
	}


	if(PRINT_PARTITION_SUMMARY) {
		if(_parallelism > 1) {
			parallel_compute_post_stats();
		} else {
			compute_post_stats();
		}
		print_partition_summary();
	}


	if(WRITE_PARTITIONS_TO_FILE) { write_post_partition_file(); }
}


/**
 * The objective here is to create the graph G such that
 * V = union of queries from (_current_iteration * _num_queries_per_iter_per_thread) to
 * ((_current_iteration + 1) * _num_queries_per_iter_per_thread) for each array in the
 * queries matrix.
 *
 * E = There exists an edge between (u, v) in V, if their weight is > 0
 *
 * Generally, this is the most expensive step in the paritioning module. So we want to
 * parallelize the graph creation step. There are a few critical assumptions that are
 * important to understand the graph creation step.
 *
 * Assume each array in the query matrix is a column: Following is a (Array index, Offset in array)
 * representation of the query matrix:
 * | (0, 0) | (1, 0) | (2, 0) | ... | (n, 0) |
 * | (0, 1) | (1, 1) | (2, 1) | ... | (n, 1) |
 * | (0, 2) | (1, 2) | (2, 2) | ... | (n, 2) |
 * | (0, 3) | (1, 3) | (2, 3) | ... | (n, 3) |
 *
 * We label our vertices 0, 1, 2, 3, ... in row-major form.
 * 0 -> 1   -> 2    -> .... -> n-1
 * n -> n+1 -> n+2  -> .... -> 2n-1
 *
 * We want to finally create the CSR representation of the entire graph. We will
 * parallelize by dividing the nodes and finding the adjacency list for each each node
 * using our existing CSRGraphCreator infrastructure.
 *
 * Each thread creates vertex weight, adjacency list and associated adjacency weight
 * array for subset of nodes they are responsible for. Finally we merge them together.
 * The only array that needs to be modified is xadj, everything is just a concatenation.
 * @return
 */
ParMETIS_CSRGraph* ParallelWorkloadPartitioner::parallel_create_graph() {

	pthread_t       threads [_parallelism];
	ThreadLocalData data    [_parallelism];
	METIS_CSRGraphCreator creators[_parallelism];
	for(uint32_t i = 0; i < _parallelism; i++) {
		data[i].fields[0] = (uint64_t) this;
		data[i].fields[1] = (uint64_t) i;
		data[i].fields[2] = (uint64_t) & creators[i];
		pthread_create(&threads[i], nullptr, parallel_create_graph_helper, (void *) &data[i]);
	}
	for(uint32_t i = 0; i < _parallelism; i++) {
		pthread_join(threads[i], nullptr);
	}

	//Initialize the graph
	auto graph = new ParMETIS_CSRGraphCreator();
	graph->begin(_parallelism);
	for(uint32_t i = 0; i < _parallelism; i++) { graph->add_graph(creators[i].get_graph()); }
	graph->finish();

	return graph->get_graph();
}

void * ParallelWorkloadPartitioner::parallel_create_graph_helper(void *data) {
	auto threadLocalData = (ThreadLocalData *) data;
	auto partitioner     = (ParallelWorkloadPartitioner *) threadLocalData->fields[0];
	auto thread_id       = (uint32_t) threadLocalData->fields[1];
	auto creator         = (METIS_CSRGraphCreator *) threadLocalData->fields[2];

	uint64_t num_global_nodes           =   partitioner->_max_cluster_graph_size;
	uint64_t num_local_nodes            =   num_global_nodes / partitioner->_parallelism;;

	uint64_t start   = thread_id * num_local_nodes;
	uint64_t end     = (thread_id + 1) * num_local_nodes;

	uint64_t num_edges = 0;
	uint64_t cross_core_weight = 0;
	uint64_t cross_core_edges = 0;

	BaseQuery *q1, *q2;
	uint32_t t1, t2;

	creator->begin(static_cast<uint32_t>(num_local_nodes));
	for(uint64_t i = start; i < end; i++) {
		t1 = partitioner->get_array_idx(i);
		partitioner->get_query(i, q1);
		creator->move_to_next_vertex();
		for(uint64_t j = 0; j < num_global_nodes; j++) {
			t2 = partitioner->get_array_idx(j);
			partitioner->get_query(j, q2);
			if(q1 != q2) {
				int weight = partitioner->compute_weight(q1, q2);
				if(weight > 0) {
					num_edges++;
					creator->add_edge(j, weight);
					if(t1 != t2) {
						cross_core_edges++;
						cross_core_weight += weight;
					}
				}
			}
		}
	}
	creator->finish();

	ATOM_ADD_FETCH(partitioner->_total_num_edges, num_edges);
	ATOM_ADD_FETCH(partitioner->_total_pre_cross_core_edges, cross_core_edges);
	ATOM_ADD_FETCH(partitioner->_total_pre_cross_core_weight, cross_core_weight);

	return nullptr;
}

METIS_CSRGraph *ParallelWorkloadPartitioner::create_graph() {
	auto creator =  new METIS_CSRGraphCreator();
	creator->begin(static_cast<uint32_t>(_max_cluster_graph_size));
	BaseQuery * q1, * q2;
	uint32_t t1, t2;
	for(uint64_t i = 0; i < _max_cluster_graph_size; i++) {
		t1 = get_array_idx(i);
		get_query(i, q1);
		creator->move_to_next_vertex();
		for(uint64_t j = 0; j < _max_cluster_graph_size; j++) {
			t2 = get_array_idx(j);
			get_query(j, q2);
			if(q1 != q2) {
				int weight = compute_weight(q1, q2);
				if(weight > 0) {
					_total_num_edges++;
					creator->add_edge(j, weight);
					if(t1 != t2) {
						_total_pre_cross_core_edges++;
						_total_pre_cross_core_weight += weight;
					}
				}
			}
		}
	}
	creator->finish();
	return creator->get_graph();
}


/*
 * We parallelize this computation using the same trick as in creating
 * the graph.
 * @param partitioner
 */
void ParallelWorkloadPartitioner::parallel_compute_post_stats() {
	pthread_t       threads [_parallelism];
	ThreadLocalData data    [_parallelism];
	for(uint32_t i = 0; i < _parallelism; i++) {
		data[i].fields[0] = (uint64_t) this;
		data[i].fields[1] = (uint64_t) i;
		pthread_create(& threads[i], nullptr, compute_statistics_helper, (void *) & data[i]);
	}
	for(uint32_t i = 0; i < _parallelism; i++) {
		pthread_join(threads[i], nullptr);
	}
}

void * ParallelWorkloadPartitioner::compute_statistics_helper(void * data) {

	auto threadLocalData = (ThreadLocalData *) data;
	auto partitioner     = (ParallelWorkloadPartitioner *) threadLocalData->fields[0];
	auto thread_id       = (uint32_t) threadLocalData->fields[1];

	uint64_t num_global_nodes           =   partitioner->_max_cluster_graph_size;
	uint64_t num_local_nodes            =   num_global_nodes / partitioner->_parallelism;

	uint64_t start   = thread_id * num_local_nodes;
	uint64_t end     = (thread_id + 1) * num_local_nodes;

	uint64_t cross_core_weight = 0;
	uint64_t cross_core_edges = 0;

	BaseQuery *q1, *q2;
	idx_t t1, t2;
	for(uint64_t i = start; i < end; i++) {
		partitioner->get_query(i, q1);
		t1 = partitioner->_current_parts[i];
		for(uint64_t j = 0; j < num_global_nodes; j++) {
			t2 = partitioner->_current_parts[j];
			partitioner->get_query(j, q2);
			if(q1 != q2) {
				int weight = partitioner->compute_weight(q1, q2);
				if(weight > 0) {
					if(t1 != t2) {
						cross_core_edges++;
						cross_core_weight += weight;
					}
				}
			}
		}
	}

	ATOM_ADD_FETCH(partitioner->_total_post_cross_core_edges, cross_core_edges);
	ATOM_ADD_FETCH(partitioner->_total_post_cross_core_weight, cross_core_weight);

	return nullptr;
}

void ParallelWorkloadPartitioner::compute_post_stats() {
	BaseQuery *q1, *q2;
	idx_t t1, t2;
	for(uint64_t i = 0; i < _max_cluster_graph_size; i++) {
		get_query(i, q1);
		t1 = _current_parts[i];
		for(uint64_t j = 0; j < _max_cluster_graph_size; j++) {
			t2 = _current_parts[j];
			get_query(j, q2);
			if(q1 != q2) {
				int weight = compute_weight(q1, q2);
				if(weight > 0) {
					if(t1 != t2) {
						_total_post_cross_core_edges++;
						_total_post_cross_core_weight += weight;
					}
				}
			}
		}
	}
}


void ParallelWorkloadPartitioner::compute_data_info() {
	//do nothing right now
}

void ParallelWorkloadPartitioner::write_pre_partition_file() {
	char file_name[100];
	sprintf(file_name, "pre_partition_%d.txt", _current_iteration);
	FILE * pre_partition_file = fopen(file_name, "w");
	for(uint32_t i = 0; i < _num_arrays; i++) {
		fprintf(pre_partition_file, "Core\t:%d\tNum Queries\t:%ld\n", (int)i, (long int)_num_queries_per_iter_per_array);
		for(auto j = static_cast<uint32_t>(_current_iteration * _num_queries_per_iter_per_array); j < (_current_iteration + 1) * _num_queries_per_iter_per_array; j++) {
			BaseQuery * query;
			_orig_queries->get(i, j, query);
			fprintf(pre_partition_file, "Transaction Id: (%d, %d)\n", (int)i, (int)j);
			print_query(pre_partition_file, query);
		}
		fprintf(pre_partition_file, "\n");
	}
	fflush(pre_partition_file);
	fclose(pre_partition_file);
}

void ParallelWorkloadPartitioner::write_post_partition_file() {
	char file_name[100];
	sprintf(file_name, "post_partition_%d.txt", _current_iteration);
	FILE * post_partition_file = fopen(file_name, "w");
	for(uint32_t i = 0; i < _num_arrays; i++) {
		uint32_t num_queries = static_cast<uint32_t>(_tmp_queries[i].size() - _tmp_array_sizes[i]);
		fprintf(post_partition_file, "Core\t:%d\tNum Queries\t:%ld\n", (int)i, static_cast<long>(num_queries));
		for(uint32_t j = _tmp_array_sizes[i]; j < (uint32_t)_tmp_queries[i].size(); j++) {
			BaseQuery * query = (BaseQuery *)_tmp_queries[i][j];
			fprintf(post_partition_file, "Transaction Id: (%d, %d)\n", (int)i, (int)j);
			print_query(post_partition_file, query);
		}
		fprintf(post_partition_file, "\n");
	}
	fflush(post_partition_file);
	fclose(post_partition_file);
}



void ParallelWorkloadPartitioner::print_partition_summary() {
	printf("******** PARTITION SUMMARY AT ITERATION %d ***********\n", _current_iteration);
	printf("%-30s: %lu\n", "Num Vertices", _max_cluster_graph_size);
	printf("%-30s: %lu\n", "Num Edges", _total_num_edges);
	printf("%-30s: %-10lu --> %lu\n", "Cross-Core Edges", _total_pre_cross_core_edges, _total_post_cross_core_edges);
	printf("%-30s: %-10lu --> %lu\n", "Cross-Core Weights", _total_pre_cross_core_weight, _total_post_cross_core_weight);
	printf("%-30s: [", "Partition Sizes");
	for(uint32_t i = 0; i < _num_arrays; i++) {
		if(i != _num_arrays - 1)
			printf("%d, ", (int)(_tmp_queries[i].size() - _tmp_array_sizes[i]));
		else
			printf("%d", (int)(_tmp_queries[i].size() - _tmp_array_sizes[i]));
	}
	printf("]\n");
}

void ParallelWorkloadPartitioner::print_execution_summary() {
	//maybe cleanup?
	uint64_t num_iterations = _current_iteration + 1;
	printf("************** EXECUTION SUMMARY **************** \n");
	printf("%-25s :: total: %10lf, avg: %10lf\n", "Obtain Data Statistics", data_statistics_duration, data_statistics_duration / num_iterations);
	printf("%-25s :: total: %10lf, avg: %10lf\n", "METIS_CSRGraph Structures Init", graph_init_duration, graph_init_duration / num_iterations);
	printf("%-25s :: total: %10lf, avg: %10lf\n", "METIS_CSRGraph Clustering", partition_duration, partition_duration / num_iterations);
	printf("%-25s :: total: %10lf, avg: %10lf\n", "Shuffle Duration", shuffle_duration, shuffle_duration / num_iterations);
	printf("************************************************* \n");
}
