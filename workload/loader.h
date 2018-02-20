#ifndef DBX1000_LOADER_H
#define DBX1000_LOADER_H

#include "query.h"
/*
 * ParallelWorkloadLoader:
 * ------------------------
 * Loads k binary files of the form <_folder_path>/core_<i>.dat that each
 * contain queries in the binary format. The loaded queries can be obtained
 * using get_query_iterator.
 */
template <typename T> class ParallelWorkloadLoader {
  public:
    ParallelWorkloadLoader(const string &folder_path, uint64_t num_threads)
        : _num_threads(num_threads), _folder_path(folder_path) {
        _queries = new Query<T> *[_num_threads];
        _array_sizes = new uint64_t[_num_threads];
    }
    void release() {
        // Implemented by derived class
        for (uint32_t i = 0; i < _num_threads; i++) {
            delete[] _queries[i];
        }
        delete[] _queries;
        delete[] _array_sizes;
    }
    void load() {
        auto threads = new pthread_t[_num_threads];
        auto data = new ThreadLocalData[_num_threads];

        uint64_t start_time = get_server_clock();
        for (auto i = 0u; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, load_helper, reinterpret_cast<void *>(&data[i]));
        }

        for (auto i = 0u; i < _num_threads; i++) {
            pthread_join(threads[i], nullptr);
        }
        uint64_t end_time = get_server_clock();
        double duration = DURATION(end_time, start_time);
        PRINT_INFO(lf, "Workload-Load-Time", duration);
        delete[] threads;
        delete[] data;
    }
    virtual QueryIterator<T> *get_queries_list(uint64_t thread_id) {
        return new QueryIterator<T>(_queries[thread_id], _array_sizes[thread_id]);
    }
    virtual QueryMatrix<T> *get_queries_matrix() {
        // Can create a query matrix only with equal number of elements in each
        // array
        uint64_t size = _array_sizes[0];
        for (uint64_t i = 0; i < _num_threads; i++) {
            assert(_array_sizes[i] == size);
        }

        return new QueryMatrix<T>(_queries, _num_threads, size);
    }

  protected:
    virtual void per_thread_load(uint32_t thread_id, FILE *file) {
        fseek(file, 0, SEEK_END);
        size_t bytes_to_read = static_cast<size_t>(ftell(file));
        fseek(file, 0, SEEK_SET);

        _array_sizes[thread_id] = bytes_to_read / sizeof(Query<T>);
        _queries[thread_id] = (Query<T> *)_mm_malloc(bytes_to_read, 64);

        size_t bytes_read =
            fread(_queries[thread_id], sizeof(Query<T>), _array_sizes[thread_id], file);
        assert(bytes_read == _array_sizes[thread_id]);
    }
    static void *load_helper(void *ptr) {
        auto data = reinterpret_cast<ThreadLocalData *>(ptr);
        auto loader = reinterpret_cast<ParallelWorkloadLoader *>(data->fields[0]);
        auto thread_id = (uint32_t)(data->fields[1]);

        // Obtain filename

        string file_name = get_workload_file_name(loader->_folder_path, thread_id);
        FILE *file = fopen(file_name.c_str(), "r");
        if (file == nullptr) {
            printf("Error opening file: %s\n", file_name.c_str());
            exit(0);
        }

        // Load from file: implemented by derived class
        loader->per_thread_load(thread_id, file);

        fclose(file);
        return nullptr;
    }
    const uint64_t _num_threads;
    const string _folder_path;
    Query<T> **_queries;
    uint64_t *_array_sizes;
};

#endif // DBX1000_LOADER_H
