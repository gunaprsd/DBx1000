
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




void WorkloadPartitioner::initialize(uint32_t num_threads,
                                     uint64_t num_params_per_thread,
                                     uint64_t num_params_pgpt,
                                     ParallelWorkloadGenerator *generator) {
    _num_threads = num_threads;
    _num_params_per_thread = num_params_per_thread;
    _num_params_pgpt = num_params_pgpt;
    assert(_num_params_per_thread % _num_params_pgpt == 0);
    _generator = generator;

    _orig_queries       = _generator->get_queries_matrix();
    _tmp_queries        = new std::vector<BaseQuery*> [_num_threads];
    _tmp_array_sizes    = new uint32_t[_num_threads];
    for(uint32_t i = 0; i < _num_threads; i++) {  _tmp_array_sizes[i] = 0; }

    num_iterations = 0;
    data_statistics_duration    = 0.0;
    graph_init_duration         = 0.0;
    partition_duration          = 0.0;
    shuffle_duration            = 0.0;
}

void WorkloadPartitioner::partition() {
    uint64_t num_params_done_pt = 0;
    num_iterations = 0;
    while(num_params_done_pt < _num_params_per_thread) {
        //specify region to read
        uint64_t start_offset = num_iterations * _num_params_pgpt;
        uint64_t num_records = min(start_offset + _num_params_pgpt, _num_params_per_thread) - start_offset;

        //create and write conflict graph
        partition_workload_part(num_iterations, num_records);

        //move to next region
        for(uint32_t i = 0; i < _num_threads; i++) { _tmp_array_sizes[i] = (uint32_t)_tmp_queries[i].size(); }
        num_params_done_pt += _num_params_pgpt;
        num_iterations++;
    }
}

void WorkloadPartitioner::partition_workload_part(uint32_t iteration, uint64_t num_records) {
    uint64_t start_time, end_time;
    auto num_total_queries = static_cast<uint32_t>(num_records * _num_threads);

    if(WRITE_PARTITIONS_TO_FILE) {
        write_pre_partition_file(iteration, num_records);
    }

    start_time = get_server_clock();
    compute_data_info(iteration, num_records);
    end_time = get_server_clock();
    data_statistics_duration += DURATION(end_time, start_time);

    start_time = get_server_clock();
    auto creator = new GraphPartitioner();
    uint32_t num_edges 						= 0;
    uint32_t pre_num_cross_edges 	= 0;
    uint32_t pre_total_weight 		= 0;
    BaseQuery *q1, *q2;
    creator->begin(num_total_queries);
    for(uint64_t i = 0; i < _num_threads; i++) {
        for(uint64_t j = iteration * num_records; j < (iteration + 1) * num_records; j++) {
            //Move to the next vertex in xadj list
            _orig_queries->get(i, j, q1);
            creator->move_to_next_vertex();

            //Now identify adjacency list for q1
            for(uint64_t k = 0; k < _num_threads; k++) {
                for(uint64_t l = iteration * num_records; l < (iteration + 1) * num_records; l++) {
                    _orig_queries->get(k, l, q2);

                    //Compute weight and add the edge
                    if(q1 != q2) {
                        int weight = compute_weight(q1, q2);
                        if(weight > 0) {
                            num_edges++;
                            auto qid = static_cast<uint32_t>(k * num_records + (l - (iteration * num_records)));
                            creator->add_edge(qid, static_cast<uint32_t>(weight));
                            if(i != k) {
                                pre_total_weight += (uint32_t)weight;
                                pre_num_cross_edges++;
                            }
                        }
                    }
                }
            }
        }
    }
    creator->finish();
    end_time = get_server_clock();
    graph_init_duration += DURATION(end_time, start_time);


    //Do clustering
    start_time = get_server_clock();
    creator->do_cluster(_num_threads);
    end_time = get_server_clock();
    partition_duration += DURATION(end_time, start_time);


    //Add query pointers into tmp_queries
    for(uint64_t i = 0; i < _num_threads; i++) {
        for(uint64_t j = iteration * num_records; j < (iteration + 1) * num_records; j++) {
            auto qid = static_cast<uint32_t>(i * num_records + (j - (iteration * num_records)));
            int partition = creator->get_cluster_id(qid);
            BaseQuery * query;
            _orig_queries->get(i, j, query);
            _tmp_queries[partition].push_back(query);
        }
    }
    creator->release();


    if(PRINT_PARTITION_SUMMARY) {
        uint32_t post_num_cross_edges 	= 0;
        uint32_t post_total_weight 			= 0;
        for(uint64_t i = 0; i < _num_threads; i++) {
            for(uint64_t j = _tmp_array_sizes[i]; j < _tmp_queries[i].size(); j++) {
                //Move to the next vertex in xadj list
                q1 = _tmp_queries[i][j];

                //Now identify adjacency list for q1
                for(uint64_t k = 0; k < _num_threads; k++) {
                    if(i != k) {
                        //Not the same thread

                        for(uint64_t l = _tmp_array_sizes[k]; l < _tmp_queries[k].size(); l++) {
                            q2 = _tmp_queries[k][l];

                            //Compute weight
                            int weight = compute_weight(q1, q2);
                            if(weight > 0) {
                                post_total_weight += (uint32_t)weight;
                                post_num_cross_edges++;
                            }
                        }
                    }
                }
            }
        }

        printf("******** PARTITION SUMMARY AT ITERATION %d ***********\n", iteration);
        printf("%-30s: %d\n", "Num Vertices", num_total_queries);
        printf("%-30s: %d\n", "Num Edges", num_edges);
        printf("%-30s: %-10d --> %d\n", "Cross-Core Edges", pre_num_cross_edges, post_num_cross_edges);
        printf("%-30s: %-10d --> %d\n", "Cross-Core Weights", pre_total_weight, post_total_weight);
        printf("%-30s: [", "Partition Sizes");
        for(uint32_t i = 0; i < _num_threads; i++) {
            if(i != _num_threads - 1)
                printf("%d, ", (int)(_tmp_queries[i].size() - _tmp_array_sizes[i]));
            else
                printf("%d", (int)(_tmp_queries[i].size() - _tmp_array_sizes[i]));
        }
        printf("]\n");
    }


    if(WRITE_PARTITIONS_TO_FILE) {
        write_post_partition_file(iteration, num_records);
    }
}

void WorkloadPartitioner::release() {
    //maybe cleanup?
    printf("*************** FINAL PARTITION SUMMARY ***************\n");
    for(uint32_t i = 0; i < _num_threads; i++) {
        printf("Thread Id: %10ld\t Queue Size: %10d\n", (long int)i, (int)_tmp_queries[i].size());
    }
    printf("************** EXECUTION SUMMARY **************** \n");
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Obtain Data Statistics", data_statistics_duration, data_statistics_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Structures Init", graph_init_duration, graph_init_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Clustering", partition_duration, partition_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Shuffle Duration", shuffle_duration, shuffle_duration / num_iterations);
    printf("************************************************* \n");
}

void WorkloadPartitioner::compute_data_info(uint32_t iteration, uint64_t num_records) {
    //do nothing right now
}

void WorkloadPartitioner::write_pre_partition_file(uint32_t iteration, uint64_t num_records) {
    char file_name[100];
    sprintf(file_name, "pre_partition_%d.txt", iteration);
    FILE * pre_partition_file = fopen(file_name, "w");
    for(uint32_t i = 0; i < _num_threads; i++) {
        fprintf(pre_partition_file, "Core\t:%d\tNum Queries\t:%ld\n", (int)i, (long int)num_records);
        for(auto j = static_cast<uint32_t>(iteration * num_records); j < (iteration + 1) * num_records; j++) {
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

void WorkloadPartitioner::write_post_partition_file(uint32_t iteration, uint64_t num_records) {
    char file_name[100];
    sprintf(file_name, "post_partition_%d.txt", iteration);
    FILE * post_partition_file = fopen(file_name, "w");
    for(uint32_t i = 0; i < _num_threads; i++) {
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

