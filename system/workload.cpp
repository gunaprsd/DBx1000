
#include <pthread.h>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "workload.h"

void ParallelWorkloadGenerator::initialize(uint32_t num_threads, uint64_t num_params_per_thread, const char * base_file_name) {
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


void ParallelWorkloadLoader::initialize(uint32_t num_threads, uint64_t num_params_per_thread, char * base_file_name) {
    _base_file_name = base_file_name;
    _num_threads = num_threads;
    _num_params_per_thread = num_params_per_thread;
    _num_params = _num_params_per_thread * _num_threads;
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


void OfflineWorkloadPartitioner::initialize(uint32_t num_threads, uint64_t num_params_per_thread, uint64_t num_params_pgpt, const char * base_file_name) {
    _num_threads = num_threads;
    _num_params_per_thread = num_params_per_thread;
    _num_params_pgpt = num_params_pgpt;
    _base_file_name = new char[100];
    strcpy(_base_file_name, base_file_name);
    open_all_files();

    num_iterations = 0;
    read_duration               = 0.0;
    write_duration              = 0.0;
    data_statistics_duration    = 0.0;
    graph_init_duration         = 0.0;
    partition_duration          = 0.0;
}

void OfflineWorkloadPartitioner::partition() {
    uint64_t num_params_done_pt = 0;
    num_iterations = 0;
    while(num_params_done_pt < _num_params_per_thread) {
        //specify region to read
        uint64_t start_offset = num_iterations * _num_params_pgpt;
        uint64_t num_records = min(start_offset + _num_params_pgpt, _num_params_per_thread) - start_offset;

        //create and write conflict graph
        partition_workload_part(num_iterations, num_records);

        //move to next region
        num_params_done_pt += _num_params_pgpt;
        num_iterations++;
    }
}

void OfflineWorkloadPartitioner::release() {
    close_all_files();
    printf("************** Workload Partition Summary **************** \n");
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Reading from File", read_duration, read_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Obtain Data Statistics", data_statistics_duration, data_statistics_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Structures Init", graph_init_duration, graph_init_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Clustering", partition_duration, partition_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Shuffle Write to File", write_duration, write_duration / num_iterations);
}

void OfflineWorkloadPartitioner::open_all_files() {
    _files      = new FILE * [_num_threads];
    _out_files  = new FILE * [_num_threads];
    char * file_name;
    char * out_file_name;

    for(uint32_t thread_id = 0; thread_id < _num_threads; thread_id++) {
        file_name       = get_workload_file(_base_file_name, thread_id);
        out_file_name   = new char[100];
        strcpy(out_file_name, "partitioned_");
        strcat(out_file_name, file_name);

        _files[thread_id]     = fopen(file_name, "r");
        _out_files[thread_id] = fopen(out_file_name, "w");

        if(_files[thread_id] == NULL) {
            printf("Error opening file: %s\n", file_name);
            exit(0);
        }

        if(_out_files[thread_id] == NULL) {
            printf("Error opening file: %s\n", out_file_name);
            exit(0);
        }

        delete file_name;
        delete out_file_name;
    }
}

void OfflineWorkloadPartitioner::close_all_files() {
    for(uint32_t thread_id = 0; thread_id < _num_threads; thread_id++) {
        fclose(_files[thread_id]);
        fclose(_out_files[thread_id]);
    }
}

void WorkloadPartitioner::initialize(uint32_t num_threads,
                                     uint64_t num_params_per_thread,
                                     uint64_t num_params_pgpt,
                                     ParallelWorkloadGenerator *generator) {
    _num_threads = num_threads;
    _num_params_per_thread = num_params_per_thread;
    _num_params_pgpt = num_params_pgpt;
    assert(_num_params_per_thread % _num_params_pgpt == 0);
    _generator = generator;
    _tmp_queries = new std::vector<BaseQuery*> [_num_threads];

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
        num_params_done_pt += _num_params_pgpt;
        num_iterations++;


        printf("***************** PARTITION SUMMARY AT ITERATION %d ******************\n", num_iterations);
        for(uint32_t i = 0; i < _num_threads; i++) {
            printf("Thread Id: %10ld\t Queue Size: %10d\n", (long int)i, (int)_tmp_queries[i].size());
        }
        printf("******************************************************\n");
    }
}

void WorkloadPartitioner::release() {
    //maybe cleanup?
    printf("***************** FINAL PARTITION SUMMARY ******************\n");
    for(uint32_t i = 0; i < _num_threads; i++) {
        printf("Thread Id: %10ld\t Queue Size: %10d\n", (long int)i, (int)_tmp_queries[i].size());
    }
    printf("******************************************************\n");
    printf("************** EXECUTION SUMMARY **************** \n");
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Obtain Data Statistics", data_statistics_duration, data_statistics_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Structures Init", graph_init_duration, graph_init_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Graph Clustering", partition_duration, partition_duration / num_iterations);
    printf("%-25s :: total: %10lf, avg: %10lf\n", "Shuffle Duration", shuffle_duration, shuffle_duration / num_iterations);
    printf("************************************************* \n");
}
