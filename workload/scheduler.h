#ifndef DBX1000_SCHEDULER_H
#define DBX1000_SCHEDULER_H

#include "loader.h"
#include "parser.h"
#include "partitioner.h"

template <typename T> class OfflineScheduler {
  public:
    OfflineScheduler(string input_folder_path, uint32_t num_threads, uint64_t max_batch_size,
                     string output_folder_path)
        : _input_folder_path(input_folder_path), _output_folder_path(output_folder_path),
          _num_threads(num_threads), _max_batch_size(max_batch_size), partitioner(nullptr) {
        // load query matrix into memory
        ParallelWorkloadLoader<T> loader(input_folder_path, num_threads);
        loader.load();
	loader.get_queries(batch, batch_size);
        loader.release();
	assert(batch != nullptr);

        if (FLAGS_parttype == "access_graph") {
            partitioner = new AccessGraphPartitioner(num_threads);
        } else if (FLAGS_parttype == "conflict_graph") {
            partitioner = new ConflictGraphPartitioner(num_threads);
        } else if (FLAGS_parttype == "heuristic1") {
            partitioner = new HeuristicPartitioner1(num_threads);
        } else if (FLAGS_parttype == "heuristic2") {
            partitioner = new HeuristicPartitioner2(num_threads);
        } else if (FLAGS_parttype == "heuristic3") {
            partitioner = new HeuristicPartitioner3(num_threads);
        } else {
            assert(false);
        }

        clusters = new vector<Query<T> *>[_num_threads];
        offset = 0;

        graph_info = new GraphInfo(AccessIterator<T>::get_max_key(), max_batch_size);
        cluster_info = new ClusterInfo(num_threads, AccessIterator<T>::get_num_tables());
	cluster_info->initialize();
	runtime_info = new RuntimeInfo();
    }
    void schedule() {
        auto thread = new pthread_t();
        auto data = new ThreadLocalData();

        data->fields[0] = (uint64_t)this;
        data->fields[1] = (uint64_t)0;
        pthread_create(thread, nullptr, schedule_helper, reinterpret_cast<void *>(data));
        pthread_join(*thread, nullptr);

        delete thread;
        delete data;
    }

  protected:
    void do_schedule() {
        uint64_t iteration = 1;
        while (offset < batch_size) {
            uint64_t size = min(_max_batch_size, (batch_size - offset));

            auto start = get_server_clock();
            create_graph_info(iteration, offset, offset + size);
            auto end = get_server_clock();
            runtime_info->rwset_duration += DURATION(end, start);
            printf("************** (Batch %lu) Input Information ***************\n", iteration);
            graph_info->print();

            partitioner->partition(iteration, graph_info, cluster_info, runtime_info);

            printf("****************** (Batch %lu) Cluster Information **************\n",
                   iteration);
            cluster_info->print();

            for (uint64_t i = 0; i < size; i++) {
                auto query = &batch[offset + i];
                clusters[graph_info->txn_info[i].assigned_core].push_back(query);
            }

            // Update temp array size parameters
            offset += size;
            iteration++;
        }

        printf("*************** Final Runtime Information *************\n");
        runtime_info->print();

        // write back onto arrays
        auto queries = new Query<T> *[_num_threads];
        auto sizes = new uint64_t[_num_threads];
        for (uint32_t i = 0; i < _num_threads; i++) {
            sizes[i] = clusters[i].size();
            queries[i] = reinterpret_cast<Query<T> *>(_mm_malloc(sizeof(Query<T>) * sizes[i], 64));
            uint32_t coffset = 0;
            for (auto query : clusters[i]) {
                auto tquery = reinterpret_cast<Query<T> *>(query);
                memcpy(&queries[i][coffset], tquery, sizeof(Query<T>));
                coffset++;
            }
        }

        // Write back onto files
        ensure_folder_exists(_output_folder_path);
        for (auto i = 0u; i < _num_threads; i++) {
            auto file_name = get_workload_file_name(_output_folder_path, i);
            FILE *file = fopen(file_name.c_str(), "w");
            if (file == nullptr) {
                printf("Unable to open file %s\n", file_name.c_str());
                exit(0);
            }

            Query<T> *thread_queries = queries[i];
            auto size = sizes[i];
            fwrite(thread_queries, sizeof(Query<T>), size, file);
            fflush(file);
            fclose(file);

#ifdef PRINT_CLUSTERED_FILE
            auto txt_file_name = file_name.substr(0, file_name.length() - 4);
            txt_file_name += ".txt";
            FILE *txt_file = fopen(txt_file_name.c_str(), "w");
            if (txt_file == nullptr) {
                printf("Unable to open file %s\n", txt_file_name.c_str());
                exit(0);
            }

            for (uint64_t j = 0; j < _temp_sizes[i]; j++) {
                print_query(txt_file, &_partitioned_queries[i][j]);
            }
            fflush(txt_file);
            fclose(txt_file);
#endif
        }
    }

    static void *schedule_helper(void *ptr) {
        set_affinity(1);
        auto data = reinterpret_cast<ThreadLocalData *>(ptr);
        auto scheduler = reinterpret_cast<OfflineScheduler *>(data->fields[0]);
        scheduler->do_schedule();
        return nullptr;
    }
    const string _input_folder_path;
    const string _output_folder_path;
    const uint32_t _num_threads;
    const uint64_t _max_batch_size;
    BasePartitioner *partitioner;
    vector<Query<T> *> *clusters;
    Query<T> *batch;
    uint64_t batch_size;
    uint64_t offset;
    GraphInfo *graph_info;
    ClusterInfo *cluster_info;
    RuntimeInfo *runtime_info;

    void create_graph_info(uint64_t iteration, uint64_t start, uint64_t end) {
        graph_info->reset();
        // Create the basic access graph
	    idx_t batch_size = (end - start);
        for (idx_t i = 0; i < batch_size; i++) {
            Query<T> *query = &batch[i + start];
            graph_info->txn_info[i].reset(i, iteration);
            ReadWriteSet *rwset = &graph_info->txn_info[i].rwset;
            query->obtain_rw_set(rwset);
            graph_info->num_txn_nodes++;

            for (uint32_t j = 0; j < rwset->num_accesses; j++) {
	            auto table_id = rwset->accesses[j].table_id;
	            auto key = rwset->accesses[j].key;
                auto info = &graph_info->data_info[key];
                if (info->epoch != iteration) {
                    // Seeing the data item for the first time
                    // in this batch - initialize appropriately
                    idx_t data_id = batch_size + graph_info->num_data_nodes;
                    info->epoch = iteration;
                    info->iteration = 0;
                    info->reset(data_id, iteration, table_id);
                    graph_info->data_inv_idx.push_back(key);
                    graph_info->num_data_nodes++;
                }

                // Add txn to read or write list of data item
                if (rwset->accesses[j].access_type == RD) {
                    info->read_txns.push_back(i);
                } else {
                    info->write_txns.push_back(i);
                }
            }

            graph_info->num_edges += rwset->num_accesses;
            ACCUMULATE_MIN(graph_info->min_txn_degree, rwset->num_accesses);
            ACCUMULATE_MAX(graph_info->max_txn_degree, rwset->num_accesses);
        }

        // Compute data min and max degrees
        assert((size_t)graph_info->num_data_nodes == graph_info->data_inv_idx.size());
        for (size_t i = 0; i < graph_info->num_data_nodes; i++) {
            auto info = &graph_info->data_info[graph_info->data_inv_idx[i]];
            auto data_degree = info->read_txns.size() + info->write_txns.size();
            ACCUMULATE_MIN(graph_info->min_data_degree, data_degree);
            ACCUMULATE_MAX(graph_info->max_data_degree, data_degree);
        }
    }
};

#endif // DBX1000_SCHEDULER_H
