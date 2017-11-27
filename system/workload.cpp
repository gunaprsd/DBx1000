
#include <pthread.h>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "workload.h"

void WorkloadGenerator::initialize(uint32_t num_threads, uint64_t num_params, char * base_file_name) {
    _base_file_name = base_file_name;
    _num_threads = num_threads;
    _num_params = num_params;
    _num_params_per_thread = _num_params / _num_threads;
    if(_base_file_name == NULL) {
        _write_to_file = false;
    }
}

void WorkloadGenerator::generate() {
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

void * WorkloadGenerator::run_helper(void *ptr) {
    ThreadLocalData * data = (ThreadLocalData *)ptr;
    WorkloadGenerator * generator = (WorkloadGenerator*)data->fields[0];
    uint32_t thread_id = (uint32_t)((uint64_t)data->fields[1]);

    generator->per_thread_generate(thread_id);
    if(generator->_write_to_file) {
        //Obtain the filename
        char * file_name = GetFileName(generator->_base_file_name, thread_id);

        //open the file
        FILE* file = fopen(file_name, "w");

        if(file == NULL) {
            printf("Error opening file: %s\n", file_name);
            exit(0);
        }

        //Write to file: one-by-one
        generator->per_thread_write_to_file(thread_id, file);

        fflush(file);
        fclose(file);
    }

    return NULL;
}


void WorkloadLoader::initialize(uint32_t num_threads, uint64_t num_params, char * base_file_name) {
    _base_file_name = base_file_name;
    _num_threads = num_threads;
    _num_params = num_params;
    _num_params_per_thread = _num_params / _num_threads;
}

void WorkloadLoader::load() {
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

void * WorkloadLoader::run_helper(void* ptr) {
    ThreadLocalData * data = (ThreadLocalData *)ptr;
    WorkloadLoader * loader = (WorkloadLoader*)data->fields[0];
    uint32_t thread_id = (uint32_t)((uint64_t)data->fields[1]);

    //Obtain the filename
    char * file_name = GetFileName(loader->_base_file_name, thread_id);

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

char * GetFileName(char * base_file_name, uint32_t thread_id)
{
    char * file_name = new char[100];
    strcpy(file_name, base_file_name);
    strcat(file_name, "_");
    sprintf(file_name + strlen(file_name), "%d", (int)thread_id);
    strcat(file_name, ".dat");
    return file_name;
}
