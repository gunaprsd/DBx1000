// Copyright[2017] <Guna Prasaad>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include "generator.h"
#include "graph_partitioner.h"


void ParallelWorkloadGenerator::initialize(uint32_t num_threads,
                                           uint64_t num_params_per_thread,
                                           const char * folder_path) {
  _num_threads = num_threads;
  _num_queries_per_thread = num_params_per_thread;
  snprintf(_folder_path, sizeof(_folder_path), "%s", folder_path);
}

void ParallelWorkloadGenerator::generate() {
  auto threads = new pthread_t[_num_threads];
  auto data = new ThreadLocalData[_num_threads];

  uint64_t start_time = get_server_clock();
  for (auto i = 0u; i < _num_threads; i++) {
    data[i].fields[0] = (uint64_t)this;
    data[i].fields[1] = (uint64_t)i;
    pthread_create(& threads[i],
                   nullptr,
                   generate_helper,
                   reinterpret_cast<void *>(& data[i]));
  }

  for (auto i = 0u; i < _num_threads; i++) {
    pthread_join(threads[i], nullptr);
  }
  uint64_t end_time = get_server_clock();
  double duration = DURATION(end_time, start_time);
  printf("Workload Generation Completed in %lf secs\n", duration);

  delete[] threads;
  delete[] data;
}

void * ParallelWorkloadGenerator::generate_helper(void *ptr) {
  auto data = reinterpret_cast<ThreadLocalData *>(ptr);
  auto loader = reinterpret_cast<ParallelWorkloadGenerator*>(
                     data->fields[0]);
  auto thread_id = (uint32_t)((uint64_t)data->fields[1]);

  loader->per_thread_generate(thread_id);

  // Obtain filename
  char file_name[200];
  get_workload_file_name(loader->_folder_path, thread_id, file_name);
  FILE* file = fopen(file_name, "w");
  if (file == nullptr) {
    printf("Error opening file: %s\n", file_name);
    exit(0);
  }

  // Write to file: implemented by derived class
  loader->per_thread_write_to_file(thread_id, file);

  fflush(file);
  fclose(file);

  return nullptr;
}

void ParallelWorkloadGenerator::release() {
  // Implemented by derived class
}



void ParallelWorkloadLoader::initialize(uint32_t num_threads,
                                        const char * folder_path) {
  snprintf(_folder_path, sizeof(_folder_path), "%s", folder_path);
  _num_threads = num_threads;
}

void ParallelWorkloadLoader::load() {
  auto threads = new pthread_t[_num_threads];
  auto data = new ThreadLocalData[_num_threads];

  uint64_t start_time = get_server_clock();
  for (auto i = 0u; i < _num_threads; i++) {
    data[i].fields[0] = (uint64_t)this;
    data[i].fields[1] = (uint64_t)i;
    pthread_create(& threads[i],
                   nullptr,
                   load_helper,
                   reinterpret_cast<void *>(& data[i]));
  }

  for (auto i = 0u; i < _num_threads; i++) {
    pthread_join(threads[i], nullptr);
  }
  uint64_t end_time = get_server_clock();
  double duration = DURATION(end_time, start_time);
  printf("Workload loading completed in %lf secs\n", duration);

  delete[] threads;
  delete[] data;
}

void * ParallelWorkloadLoader::load_helper(void *ptr) {
  auto data = reinterpret_cast<ThreadLocalData *>(ptr);
  auto loader = reinterpret_cast<ParallelWorkloadLoader*>(
                     data->fields[0]);
  auto thread_id = (uint32_t)(data->fields[1]);

  // Obtain filename
  char file_name[200];
  get_workload_file_name(loader->_folder_path, thread_id, file_name);
  FILE* file = fopen(file_name, "r");
  if (file == nullptr) {
    printf("Error opening file: %s\n", file_name);
    exit(0);
  }

  // Load from file: implemented by derived class
  loader->per_thread_load(thread_id, file);

  fclose(file);
  return nullptr;
}

void ParallelWorkloadLoader::release() {
  // Implemented by derived class
}
