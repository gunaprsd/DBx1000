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
	  auto start_time = get_server_clock();
            FILE *file = fopen(_file_path.c_str(), "r");
            if (file == nullptr) {
                printf("Error opening workload file: %s\n", _file_path.c_str());
                exit(0);
            }

            _meta_data.read(file);
            auto num_queries_to_read = _meta_data.get_total_num_queries();
            _queries = new Query<T>[num_queries_to_read];
            auto num_queries_read =
                fread(_queries, sizeof(Query<T>), static_cast<size_t>(num_queries_to_read), file);

            if (num_queries_read != num_queries_to_read) {
                printf("Workload file corrupt or incomplete!\n");
                exit(0);
            }
	    auto end_time = get_server_clock();
	    auto duration = DURATION(end_time, start_time);
	    printf("Workload loading completed in %lf secs\n", duration);
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
    const string _file_path;
    Query<T> *_queries;
    WorkloadMetaData _meta_data;
};

#endif // DBX1000_LOADER_H
