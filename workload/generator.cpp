
#include <pthread.h>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "generator.h"
#include "graph_partitioner.h"


void ParallelWorkloadGenerator::initialize(uint32_t num_threads,
                                           uint64_t num_params_per_thread,
                                           const char * folder_path) {
    _num_threads = num_threads;
    _num_queries_per_thread = num_params_per_thread;
    strcpy(_folder_path, folder_path);
}

void ParallelWorkloadGenerator::generate() {
    pthread_t threads[_num_threads];
    ThreadLocalData data[_num_threads];

    uint64_t start_time = get_server_clock();
    for(uint32_t i = 0; i < _num_threads; i++) {
        data[i].fields[0] = (uint64_t)this;
        data[i].fields[1] = (uint64_t)i;
        pthread_create(&threads[i], nullptr, generate_helper, (void *) &data[i]);
    }

    for(uint32_t i = 0; i < _num_threads; i++) {
        pthread_join(threads[i], nullptr);
    }

    uint64_t end_time = get_server_clock();
    double duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
    printf("Workload Generation Completed in %lf secs\n", duration);
}

void * ParallelWorkloadGenerator::generate_helper(void *ptr) {
    auto data = (ThreadLocalData *)ptr;
    auto generator = (ParallelWorkloadGenerator*)data->fields[0];
    auto thread_id = (uint32_t)((uint64_t)data->fields[1]);

    generator->per_thread_generate(thread_id);

    //Obtain the filename
    char file_name[200];
    get_workload_file_name(generator->_folder_path, thread_id, file_name);
    FILE* file = fopen(file_name, "w");
    if(file == nullptr) {
        printf("Error opening file: %s\n", file_name);
        exit(0);
    }

    //Write to file: one-by-one
    generator->per_thread_write_to_file(thread_id, file);

    fflush(file);
    fclose(file);

    return nullptr;
}

void ParallelWorkloadGenerator::release() {
    //Must be implemented in derived class
}



void ParallelWorkloadLoader::initialize(uint32_t num_threads,
                                        const char * folder_path) {
    strcpy(_folder_path, folder_path);
    _num_threads = num_threads;
}

void ParallelWorkloadLoader::load() {
    pthread_t threads[_num_threads];
    ThreadLocalData data[_num_threads];

    uint64_t start_time = get_server_clock();
    for(uint32_t i = 0; i < _num_threads; i++) {
        data[i].fields[0] = (uint64_t)this;
        data[i].fields[1] = (uint64_t)i;
        pthread_create(&threads[i], NULL, load_helper, (void *) &data[i]);
    }

    for(uint32_t i = 0; i < _num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t end_time = get_server_clock();
    double duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
    printf("Workload Loading Completed in %lf secs\n", duration);
}

void * ParallelWorkloadLoader::load_helper(void *ptr) {
    auto data = (ThreadLocalData *)ptr;
    auto loader = (ParallelWorkloadLoader*)data->fields[0];
    auto thread_id = (uint32_t)((uint64_t)data->fields[1]);

    //Obtain the filename
    char file_name[200];
    get_workload_file_name(loader->_folder_path, thread_id, file_name);
    
    //open the file
    FILE* file = fopen(file_name, "r");

    if(file == NULL) {
        printf("Error opening file: %s\n", file_name);
        exit(0);
    }

    //Write to file: one-by-one
    loader->per_thread_load(thread_id, file);

    fclose(file);
    return nullptr;
}

void ParallelWorkloadLoader::release() {
    //Need to release all resources held by loader
    //implemented in derived class
}


