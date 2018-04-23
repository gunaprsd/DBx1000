#ifndef DBX1000_LOADER_H
#define DBX1000_LOADER_H

#include "query.h"

template <typename T> class WorkloadLoader {
  public:
    WorkloadLoader(const string &file_path)
        : _file_path(file_path), _queries(nullptr), _meta_data() {}

    ~WorkloadLoader() { delete[] _queries; }

    void load() {
        if (_queries == nullptr) {

            FILE *file = fopen(_file_path.c_str(), "r");
            if (file == nullptr) {
                printf("Error opening workload file: %s\n", _file_path.c_str());
                exit(0);
            }
            _meta_data.read(file);
            auto num_queries_to_read = _meta_data.get_total_num_queries();
            _queries = new Query<T>[num_queries_to_read];
            fclose(file);

            auto start_time = get_server_clock();
            auto threads = new pthread_t[_meta_data.num_threads];
            auto data = new ThreadLocalData[_meta_data.num_threads];

            for (auto i = 0u; i < _meta_data.num_threads; i++) {
                data[i].fields[0] = (uint64_t)this;
                data[i].fields[1] = (uint64_t)i;
                pthread_create(&threads[i], nullptr, load_helper,
                               reinterpret_cast<void *>(&data[i]));
            }

            for (auto i = 0u; i < _meta_data.num_threads; i++) {
                pthread_join(threads[i], nullptr);
            }
            auto end_time = get_server_clock();
            auto duration = DURATION(end_time, start_time);
            printf("Workload loading completed in %lf secs\n", duration);

            delete[] threads;
            delete[] data;
        } else {
            printf("Workload already read into memory!\n");
            exit(0);
        }
    }

    QueryIterator<T> *get_queries_list(uint64_t thread_id) {
        Query<T> *thread_query_pointer = _queries + _meta_data.index[thread_id];
        uint64_t size = _meta_data.get_num_queries(thread_id);
        return new QueryIterator<T>(thread_query_pointer, size);
    }

    void get_queries(Query<T> *&batch, uint64_t &batch_size) {
        batch = _queries;
        batch_size = static_cast<uint64_t>(_meta_data.get_total_num_queries());
    }

    uint64_t get_num_threads() { return _meta_data.num_threads; }

  protected:
    void per_thread_load(uint32_t thread_id) {
        FILE *file = fopen(_file_path.c_str(), "r");
        if (file == nullptr) {
            printf("Error opening workload file: %s\n", _file_path.c_str());
            exit(0);
        }

        auto start_offset =
            sizeof(WorkloadMetaData) + sizeof(Query<T>) * _meta_data.index[thread_id];
        auto num_queries = _meta_data.get_num_queries(thread_id);

        auto array_start_offset = _meta_data.index[thread_id];
        auto query_ptr = _queries + array_start_offset;

        fseek(file, start_offset, SEEK_SET);
        auto num_queries_read = fread(query_ptr, sizeof(Query<T>), num_queries, file);

        if (num_queries_read != num_queries) {
            printf("[tid=%u] Could not read compeletely!\n", thread_id);
            exit(0);
        }

        fclose(file);
    }

    static void *load_helper(void *ptr) {
        auto data = reinterpret_cast<ThreadLocalData *>(ptr);
        auto loader = reinterpret_cast<WorkloadLoader<T> *>(data->fields[0]);
        auto thread_id = (uint32_t)((uint64_t)data->fields[1]);
        set_affinity(thread_id);
        loader->per_thread_load(thread_id);
        return nullptr;
    }
    const string _file_path;
    Query<T> *_queries;
    WorkloadMetaData _meta_data;
};

#endif // DBX1000_LOADER_H
