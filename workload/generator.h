// Copyright[2017] <Guna Prasaad>

#ifndef __PARALLEL_WORKLOAD_GENERATOR_H__
#define __PARALLEL_WORKLOAD_GENERATOR_H__

#include "global.h"
#include "helper.h"
#include "query.h"

template <typename T> class ParallelWorkloadGenerator {
  public:
    ParallelWorkloadGenerator(uint64_t num_threads, uint64_t num_queries_per_thread,
                              const string &file_name)
        : _num_threads(num_threads), _num_queries_per_thread(num_queries_per_thread),
          _file_name(file_name) {
        _queries = new Query<T> *[_num_threads];
        for (uint32_t i = 0; i < _num_threads; i++) {
            _queries[i] = new Query<T>[_num_queries_per_thread];
        }
    }

    void generate() {
        parallel_do_generate();
        write_to_file();
    }

    virtual ~ParallelWorkloadGenerator() {
        for (uint32_t i = 0; i < _num_threads; i++) {
            delete[] _queries[i];
        }
        delete[] _queries;
    }

  protected:
    void write_to_file() {
        // Create a file
        FILE *file = fopen(_file_name.c_str(), "w");
        if (file == nullptr) {
            printf("Error opening file: %s\n", _file_name.c_str());
            exit(0);
        }

        // Write all queries sequentially
        WorkloadMetaData metadata(_num_threads, _num_queries_per_thread);
        metadata.write(file);
        for (uint64_t i = 0; i < _num_threads; i++) {
            fwrite(_queries[i], sizeof(Query<T>), _num_queries_per_thread, file);
        }

        fflush(file);
        fclose(file);
    }

    void parallel_do_generate() {
        auto threads = new pthread_t[_num_threads];
        auto data = new ThreadLocalData[_num_threads];

        uint64_t start_time = get_server_clock();
        for (auto i = 0u; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, generate_helper,
                           reinterpret_cast<void *>(&data[i]));
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

    const uint64_t _num_threads;
    const uint64_t _num_queries_per_thread;
    const string _file_name;
    Query<T> **_queries;

  private:
    static void *generate_helper(void *ptr) {
        auto data = reinterpret_cast<ThreadLocalData *>(ptr);
        auto generator = reinterpret_cast<ParallelWorkloadGenerator *>(data->fields[0]);
        auto thread_id = (uint32_t)((uint64_t)data->fields[1]);
        set_affinity(thread_id);
        generator->per_thread_generate(thread_id);
        return nullptr;
    }

    virtual void per_thread_generate(uint64_t thread_id) = 0;
};

#endif // __PARALLEL_WORKLOAD_GENERATOR_H__
