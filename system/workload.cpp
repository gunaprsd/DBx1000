
#include <pthread.h>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "workload.h"
#include "graph_partitioner.h"


void ParallelWorkloadGenerator::initialize(uint32_t num_threads,
                                           uint64_t num_params_per_thread,
                                           const char * base_file_name) {
    if(base_file_name == nullptr) {
        _write_to_file = false;
        _base_file_name = nullptr;
    } else {
        _write_to_file = true;
        _base_file_name = new char[100];
        strcpy(_base_file_name, base_file_name);
    }
    _num_threads = num_threads;
    _num_params_per_thread = num_params_per_thread;
    _num_params = _num_params_per_thread * _num_threads;
}

void ParallelWorkloadGenerator::generate() {
    pthread_t threads[_num_threads];
    ThreadLocalData data[_num_threads];

    uint64_t start_time = get_server_clock();
    for(uint32_t i = 0; i < _num_threads; i++) {
        data[i].fields[0] = (uint64_t)this;
        data[i].fields[1] = (uint64_t)i;
        pthread_create(&threads[i], NULL, run_helper, (void *)& data[i]);
    }

    for(uint32_t i = 0; i < _num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t end_time = get_server_clock();
    double duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
    printf("Workload Generation Completed in %lf secs\n", duration);
}

void * ParallelWorkloadGenerator::run_helper(void *ptr) {
    ThreadLocalData * data = (ThreadLocalData *)ptr;
    ParallelWorkloadGenerator * generator = (ParallelWorkloadGenerator*)data->fields[0];
    uint32_t thread_id = (uint32_t)((uint64_t)data->fields[1]);

    generator->per_thread_generate(thread_id);
    if(generator->_write_to_file) {
        //Obtain the filename
        char * file_name = get_workload_file(generator->_base_file_name, thread_id);

        //open the file
        FILE* file = fopen(file_name, "w");

        if(file == nullptr) {
            printf("Error opening file: %s\n", file_name);
            exit(0);
        }

        //Write to file: one-by-one
        generator->per_thread_write_to_file(thread_id, file);

        fflush(file);
        fclose(file);
    }

    return nullptr;
}

void ParallelWorkloadGenerator::release() {}



void ParallelWorkloadLoader::initialize(uint32_t num_threads,
                                        char * base_file_name) {
    _base_file_name = base_file_name;
    _num_threads = num_threads;
}

void ParallelWorkloadLoader::load() {
    pthread_t threads[_num_threads];
    ThreadLocalData data[_num_threads];

    uint64_t start_time = get_server_clock();
    for(uint32_t i = 0; i < _num_threads; i++) {
        data[i].fields[0] = (uint64_t)this;
        data[i].fields[1] = (uint64_t)i;
        pthread_create(&threads[i], NULL, run_helper, (void *)&data[i]);
    }

    for(uint32_t i = 0; i < _num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t end_time = get_server_clock();
    double duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
    printf("Workload Loading Completed in %lf secs", duration);
}

void * ParallelWorkloadLoader::run_helper(void* ptr) {
    ThreadLocalData * data = (ThreadLocalData *)ptr;
    ParallelWorkloadLoader * loader = (ParallelWorkloadLoader*)data->fields[0];
    uint32_t thread_id = (uint32_t)((uint64_t)data->fields[1]);

    //Obtain the filename
    char * file_name = get_workload_file(loader->_base_file_name, thread_id);
    
    //open the file
    FILE* file = fopen(file_name, "r");

    if(file == NULL) {
        printf("Error opening file: %s\n", file_name);
        exit(0);
    }

    //Write to file: one-by-one
    loader->per_thread_load(thread_id, file);

    fclose(file);

    return NULL;
}

void ParallelWorkloadLoader::release() {}




void ParallelWorkloadPartitioner::initialize(BaseQueryMatrix * queries,
                                             uint64_t max_cluster_graph_size,
                                             uint32_t parallelism)
{

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

void ParallelWorkloadPartitioner::partition_per_iteration()
{
    uint64_t start_time, end_time;
    if(WRITE_PARTITIONS_TO_FILE) {	write_pre_partition_file(); }

    start_time = get_server_clock();
    compute_data_info();
    end_time = get_server_clock();
    data_statistics_duration += DURATION(end_time, start_time);

    start_time = get_server_clock();
    auto graph = parallel_create_graph();
    //auto graph = create_graph();
    end_time = get_server_clock();
    graph_init_duration += DURATION(end_time, start_time);

    //Do clustering
    start_time = get_server_clock();
    METISGraphPartitioner partitioner;
    partitioner.partition(graph, _num_arrays);
    end_time = get_server_clock();
    partition_duration += DURATION(end_time, start_time);


    //Add query pointers into tmp_queries
    BaseQuery * query = nullptr;
    int partition;
    for(uint64_t i = 0; i < _max_cluster_graph_size; i++) {
        partition = partitioner.get_partition(i);
        get_query(i, query);
        _tmp_queries[partition].push_back(query);
    }


    if(PRINT_PARTITION_SUMMARY) {
        parallel_compute_post_stats(& partitioner);
        print_partition_summary();
    }


    if(WRITE_PARTITIONS_TO_FILE) { write_post_partition_file(); }
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

void ParallelWorkloadPartitioner::write_to_files(const char * base_file_name) {
    for(uint32_t i = 0; i < _num_arrays; i++) {
        char file_name[200];
        sprintf(file_name, "%s/core_%d.dat", base_file_name, (int)i);
        FILE * file = fopen(file_name, "w");
        write_workload_file(i, file);
        fflush(file);
        fclose(file);
    }
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
Graph * ParallelWorkloadPartitioner::parallel_create_graph() {

    pthread_t       threads [_parallelism];
    ThreadLocalData data    [_parallelism];
    CSRGraphCreator creators[_parallelism];
    for(uint32_t i = 0; i < _parallelism; i++) {
        data[i].fields[0] = (uint64_t) this;
        data[i].fields[1] = (uint64_t) i;
        data[i].fields[2] = (uint64_t) & creators[i];
        pthread_create(& threads[i], nullptr, create_graph_helper, (void *) & data[i]);
    }
    for(uint32_t i = 0; i < _parallelism; i++) {
        pthread_join(threads[i], nullptr);
    }

    //Compute adjacency list size
    uint64_t adjncy_size = 0;
    for(uint32_t i = 0; i < _parallelism; i++) { adjncy_size += creators[i].get_graph()->adjncy_size; }

    //Initialize the graph
    auto graph = new Graph();
    graph->ncon      = 1;
    graph->nvtxs     = _max_cluster_graph_size;
    graph->adjncy_size = adjncy_size;

    graph->xadj      = (idx_t *) malloc(sizeof(idx_t) * (graph->nvtxs + 1));
    graph->vwgt      = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);
    graph->adjncy    = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);
    graph->adjwgt    = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);

    int vtx_array_offset = 0;
    int adj_array_offset = 0;
    for(uint32_t i = 0; i < _parallelism; i++) {
        Graph * pseudo_sub_graph = creators[i].get_graph();

        //Copy arrays that don't change
        memcpy(& graph->vwgt[vtx_array_offset], pseudo_sub_graph->vwgt, sizeof(idx_t) * pseudo_sub_graph->nvtxs);
        memcpy(& graph->adjncy[adj_array_offset], pseudo_sub_graph->adjncy, sizeof(idx_t) * pseudo_sub_graph->adjncy_size);
        memcpy(& graph->adjwgt[adj_array_offset], pseudo_sub_graph->adjwgt, sizeof(idx_t) * pseudo_sub_graph->adjncy_size);

        //Copy and adjust xadj array
        memcpy(& graph->xadj[vtx_array_offset], pseudo_sub_graph->xadj, sizeof(idx_t) * pseudo_sub_graph->nvtxs);
        for(uint32_t j = 0; j < pseudo_sub_graph->nvtxs; j++) {
            graph->xadj[j + vtx_array_offset] += adj_array_offset;
        }

        vtx_array_offset += pseudo_sub_graph->nvtxs;
        adj_array_offset += pseudo_sub_graph->adjncy_size;
    }
    graph->xadj[graph->nvtxs] = adj_array_offset;

    for(uint32_t i = 0; i < _parallelism; i++) { creators[i].get_graph()->release(); }

    return graph;
}

void * ParallelWorkloadPartitioner::create_graph_helper(void * data) {
    auto threadLocalData = (ThreadLocalData *) data;
    auto partitioner     = (ParallelWorkloadPartitioner *) threadLocalData->fields[0];
    auto thread_id       = (uint32_t) threadLocalData->fields[1];
    auto creator         = (CSRGraphCreator *) threadLocalData->fields[2];

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


Graph *ParallelWorkloadPartitioner::create_graph() {
    auto creator =  new CSRGraphCreator();
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
void ParallelWorkloadPartitioner::parallel_compute_post_stats(METISGraphPartitioner * partitioner) {
    pthread_t       threads [_parallelism];
    ThreadLocalData data    [_parallelism];
    for(uint32_t i = 0; i < _parallelism; i++) {
        data[i].fields[0] = (uint64_t) this;
        data[i].fields[1] = (uint64_t) i;
        data[i].fields[2] = (uint64_t) partitioner;
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
    auto computed_partitions = (METISGraphPartitioner *) threadLocalData->fields[2];

    uint64_t num_global_nodes           =   partitioner->_max_cluster_graph_size;
    uint64_t num_local_nodes            =   num_global_nodes / partitioner->_parallelism;
    
    uint64_t start   = thread_id * num_local_nodes;
    uint64_t end     = (thread_id + 1) * num_local_nodes;

    uint64_t cross_core_weight = 0;
    uint64_t cross_core_edges = 0;

    BaseQuery *q1, *q2;
    uint32_t t1, t2;
    for(uint64_t i = start; i < end; i++) {
        partitioner->get_query(i, q1);
        t1 = computed_partitions->get_partition(i);
        for(uint64_t j = 0; j < num_global_nodes; j++) {
            t2 = computed_partitions->get_partition(j);
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
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Structures Init", graph_init_duration, graph_init_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Clustering", partition_duration, partition_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Shuffle Duration", shuffle_duration, shuffle_duration / num_iterations);
    printf("************************************************* \n");
}
