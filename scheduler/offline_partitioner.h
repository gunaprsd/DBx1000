#ifndef __OFFLINE_PARTITIONER_H__
#define __OFFLINE_PARTITIONER_H__

#include "loader.h"
#include "parser.h"
#include "partitioner.h"

template <typename T> class OfflinePartitioner {
  public:
    OfflinePartitioner(string input_file, uint32_t num_threads, string output_file)
        : _input_file(input_file), _output_file(output_file), _num_threads(num_threads),
          _partitioner(nullptr), _loader(nullptr) {

        create_data_txns_map = true;
        if (FLAGS_parttype == "access_graph") {
            _partitioner = new AccessGraphPartitioner(num_threads);
        } else if (FLAGS_parttype == "conflict_graph") {
            _partitioner = new ConflictGraphPartitioner(num_threads);
        } else if (FLAGS_parttype == "heuristic1") {
            _partitioner = new HeuristicPartitioner1(num_threads);
        } else if (FLAGS_parttype == "heuristic2") {
            _partitioner = new HeuristicPartitioner2(num_threads);
        } else if (FLAGS_parttype == "heuristic3") {
            _partitioner = new HeuristicPartitioner3(num_threads);
        } else if (FLAGS_parttype == "kmeans") {
            _partitioner = new KMeansPartitioner(num_threads);
        } else if (FLAGS_parttype == "bfs") {
            _partitioner = new BreadthFirstSearchPartitioner(num_threads);
        } else if (FLAGS_parttype == "union_find") {
            _partitioner = new UnionFindPartitioner(num_threads);
            create_data_txns_map = false;
        } else if (FLAGS_parttype == "parallel_union_find") {
            _partitioner = new ParallelUnionFindPartitioner(num_threads, num_threads);
            create_data_txns_map = false;
        } else if (FLAGS_parttype == "random") {
            _partitioner = new RandomPartitioner(num_threads);
            create_data_txns_map = false;
        } else if (FLAGS_parttype == "dummy") {
            _partitioner = new DummyPartitioner(num_threads);
            create_data_txns_map = false;
        } else {
            assert(false);
        }
    }

    void schedule() {
        load_from_file();
        create_graph_info();
        partition();
        write_to_file();
        print_stats();
    }

  protected:

    void print_stats() {
        printf("*** Input Information ***\n");
        graph_info->print();

        printf("*** Cluster Information ***\n");
        cluster_info->print();

        printf("*** Runtime Information ***\n");
        runtime_info->print();
    }

    void load_from_file() {
        _loader = new WorkloadLoader<T>(_input_file);
        _loader->load();
        _loader->get_queries(_batch, _batch_size);

        if(_loader->get_num_threads() != _num_threads) {
        	printf("Workload arity does not match!\n");
        	exit(0);
        }
    }

    void partition() {
        _partitioner->partition(1, graph_info, cluster_info, runtime_info);
    }

    /*
     * If create_data_txns_map is set, then the read_txns and write_txns are created.
     * Else, everything else is created:
     * 1. num_txn_nodes
     * 2. num_data_nodes
     * 3. txn_info[0 ... (num_txn_nodes - 1)]
     * 4. data_info[0 ... (num_data_nodes - 1)]
     */
    void create_graph_info() {
        auto start_time = get_server_clock();

        graph_info = new GraphInfo(AccessIterator<T>::get_max_key(), _batch_size);
        cluster_info = new ClusterInfo(_num_threads, AccessIterator<T>::get_num_tables());
        cluster_info->initialize();
        runtime_info = new RuntimeInfo();

        graph_info->reset();
        for (uint64_t i = 0; i < _batch_size; i++) {
            Query<T> *query = &_batch[i];
            graph_info->txn_info[i].reset(i, 1);
            ReadWriteSet *rwset = &(graph_info->txn_info[i].rwset);
            query->obtain_rw_set(rwset);

            graph_info->num_txn_nodes++;
            graph_info->num_edges += rwset->num_accesses;
            ACCUMULATE_MIN(graph_info->min_txn_degree, rwset->num_accesses);
            ACCUMULATE_MAX(graph_info->max_txn_degree, rwset->num_accesses);

            for (uint32_t j = 0; j < rwset->num_accesses; j++) {
                auto table_id = rwset->accesses[j].table_id;
                auto key = rwset->accesses[j].key;
                auto info = &(graph_info->data_info[key]);

                if (info->epoch != UINT64_MAX) {
                    idx_t data_id = _batch_size + graph_info->num_data_nodes;
                    info->epoch = UINT64_MAX;
                    info->iteration = 0;
                    info->reset(data_id, UINT64_MAX, table_id);
                    graph_info->data_inv_idx.push_back(key);
                    graph_info->num_data_nodes++;
                }

                if (rwset->accesses[j].access_type == RD) {
                    info->num_read_txns++;
                    if (create_data_txns_map) {
                        info->read_txns.push_back(i);
                    }
                } else {
                    info->num_write_txns++;
                    if (create_data_txns_map) {
                        info->write_txns.push_back(i);
                    }
                }
            }
        }

        for (size_t i = 0; i < graph_info->num_data_nodes; i++) {
            auto info = &graph_info->data_info[graph_info->data_inv_idx[i]];
            auto data_degree = info->read_txns.size() + info->write_txns.size();
            ACCUMULATE_MIN(graph_info->min_data_degree, data_degree);
            ACCUMULATE_MAX(graph_info->max_data_degree, data_degree);
        }

        auto end_time = get_server_clock();
        runtime_info->rwset_duration += DURATION(end_time, start_time);
    }

    void write_to_file() {
        auto clusters = new vector<Query<T>*>[_num_threads];
        for (uint64_t i = 0; i < _batch_size; i++) {
            auto query = &(_batch[i]);
            auto txn_info = &(graph_info->txn_info[i]);
            auto core = txn_info->assigned_core;
            clusters[core].push_back(query);
        }

        vector<uint64_t> sizes;
        auto queries = new Query<T> *[_num_threads];
        for (uint32_t i = 0; i < _num_threads; i++) {
            sizes.push_back(clusters[i].size());
            queries[i] = reinterpret_cast<Query<T> *>(_mm_malloc(sizeof(Query<T>) * sizes[i], 64));
            uint32_t coffset = 0;
            for (auto query : clusters[i]) {
                auto tquery = reinterpret_cast<Query<T>*>(query);
                memcpy(&queries[i][coffset], tquery, sizeof(Query<T>));
                coffset++;
            }
        }

        FILE *file = fopen(_output_file.c_str(), "w");
        if (file == nullptr) {
            printf("Error opening file: %s\n", _output_file.c_str());
            exit(0);
        }

        WorkloadMetaData metadata(_num_threads, sizes);
        metadata.write(file);
        for (uint64_t i = 0; i < metadata.num_threads; i++) {
            fwrite(queries[i], sizeof(Query<T>), sizes[i], file);
        }

        fflush(file);
        fclose(file);
    }

  private:
    const string _input_file;
    const string _output_file;
    const uint32_t _num_threads;
    BasePartitioner *_partitioner;
    WorkloadLoader<T>* _loader;
    Query<T> *_batch;
    uint64_t _batch_size;
    GraphInfo *graph_info;
    ClusterInfo *cluster_info;
    RuntimeInfo *runtime_info;
    bool create_data_txns_map;
};

#endif // __OFFLINE_PARTITIONER_H__
