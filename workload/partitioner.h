#ifndef DBX1000_PARTITIONER_H
#define DBX1000_PARTITIONER_H
#include "global.h"
#include "query.h"
#include "graph_partitioner.h"

template <typename T> class BasePartitioner {
public:
  virtual void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) = 0;
};

template <typename T> class AccessGraphPartitioner : public BasePartitioner<T> {
  struct RuntimeStatistics {
    double first_pass_duration;
    double second_pass_duration;
    double third_pass_duration;
    double partition_duration;
    RuntimeStatistics() { reset(); }
    void reset() {
      first_pass_duration = 0;
      second_pass_duration = 0;
      third_pass_duration = 0;
      partition_duration = 0;
    }
    void print() {
      printf("%-30s: %10lf\n", "First-Pass-Duration", first_pass_duration);
      printf("%-30s: %10lf\n", "Second-Pass-Duration", second_pass_duration);
      printf("%-30s: %10lf\n", "Third-Pass-Duration", third_pass_duration);
      printf("%-30s: %10lf\n", "Partition-Duration", partition_duration);
    }
  };
  struct InputStatistics {
    uint64_t num_txn_nodes;
    uint64_t num_data_nodes;
    uint64_t num_edges;
    uint64_t min_data_degree;
    uint64_t max_data_degree;
    InputStatistics() { reset(); }
    void reset() {
      num_txn_nodes = 0;
      num_data_nodes = 0;
      num_edges = 0;
      min_data_degree = 0;
      max_data_degree = 0;
    }
    void print() {
      printf("%-30s: %lu\n", "Num-Data-Nodes", num_data_nodes);
      printf("%-30s: %lu\n", "Num-Txn-Nodes", num_txn_nodes);
      printf("%-30s: %lu\n", "Num-Edges", num_edges);
      printf("%-30s: %lu\n", "Min-Data-Degree", min_data_degree);
      printf("%-30s: %lu\n", "Max-Data-Degree", max_data_degree);
    }
  };
  struct ClusterStatistics {
    uint64_t min_data_core_degree;
    uint64_t max_data_core_degree;
    uint64_t min_txn_cross_access;
    uint64_t max_txn_cross_access;
    uint64_t total_cross_access;
    ClusterStatistics() { reset(); }
    void reset() {
      min_data_core_degree = 0;
      max_data_core_degree = 0;
      min_txn_cross_access = 0;
      max_txn_cross_access = 0;
      total_cross_access = 0;
    }
  };
  struct OutputStatistics {
    ClusterStatistics random_cluster;
    ClusterStatistics output_cluster;
    OutputStatistics() : random_cluster(), output_cluster() {}
    void reset() {
      random_cluster.reset();
      output_cluster.reset();
    }
    void print() {
      printf("%-30s: %lu\n", "Rnd-Min-Data-Core-Degree",
             random_cluster.min_data_core_degree);
      printf("%-30s: %lu\n", "Rnd-Min-Data-Core-Degree",
             random_cluster.max_data_core_degree);
      printf("%-30s: %lu\n", "Rnd-Min-Txn-Cross-Access",
             random_cluster.min_txn_cross_access);
      printf("%-30s: %lu\n", "Rnd-Max-Txn-Cross-Access",
             random_cluster.max_txn_cross_access);
      printf("%-30s: %lu\n", "Rnd-Total-Txn-Cross-Access",
             random_cluster.total_cross_access);
      printf("%-30s: %lu\n", "Min-Data-Core-Degree",
             output_cluster.min_data_core_degree);
      printf("%-30s: %lu\n", "Min-Data-Core-Degree",
             output_cluster.max_data_core_degree);
      printf("%-30s: %lu\n", "Min-Txn-Cross-Access",
             output_cluster.min_txn_cross_access);
      printf("%-30s: %lu\n", "Max-Txn-Cross-Access",
             output_cluster.max_txn_cross_access);
      printf("%-30s: %lu\n", "Total-Txn-Cross-Access",
             output_cluster.total_cross_access);
    }
  };
public:
  AccessGraphPartitioner(uint32_t num_clusters) : _num_clusters(num_clusters), _batch(nullptr), _info_array(nullptr),
                                                  vwgt(), adjwgt(), xadj(), adjncy(), iteration(0), runtime_stats(),
                                                  input_stats(), output_stats()
  {
    uint64_t size = AccessIterator<T>::getMaxKey();
    _info_array = new TxnDataInfo[size];
  }
  void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) override {
    _batch = batch;
    iteration++;
    // reset all parameters

    uint64_t start_time, end_time;
    double duration;

    start_time = get_server_clock();
    first_pass();
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.first_pass_duration = duration;

    start_time = get_server_clock();
    second_pass();
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.second_pass_duration = duration;

    start_time = get_server_clock();
    third_pass();
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.third_pass_duration = duration;

    auto graph = new METIS_CSRGraph();
    uint64_t total_num_vertices =
            input_stats.num_txn_nodes + input_stats.num_data_nodes;
    graph->nvtxs = total_num_vertices;
    graph->adjncy_size = 2 * input_stats.num_edges;
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->ncon = 1;

    auto parts = new idx_t[total_num_vertices];
    for (uint32_t i = 0; i < total_num_vertices; i++) {
      parts[i] = -1;
    }

    start_time = get_server_clock();
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, parts);
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.partition_duration = duration;

    // compute partition stats
    compute_partition_stats(parts, output_stats.output_cluster);
    partitions.reserve(input_stats.num_txn_nodes);
    for (uint64_t i = 0; i < input_stats.num_txn_nodes; i++) {
      partitions.push_back(parts[i]);
    }

    // compute stats for random allotment
    for (size_t i = 0; i < total_num_vertices; i++) {
      parts[i] = rand() % _num_clusters;
    }
    compute_partition_stats(parts, output_stats.random_cluster);

    delete graph;
    delete[] parts;
    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();
  }
  void print_stats() {
    printf("%-30s: %lu\n", "Iteration", iteration);
    input_stats.print();
    runtime_stats.print();
    output_stats.print();
  }
protected:
  const uint32_t _num_clusters;
  QueryBatch<T> *_batch;
  TxnDataInfo *_info_array;
  vector<idx_t> vwgt;
  vector<idx_t> adjwgt;
  vector<idx_t> xadj;
  vector<idx_t> adjncy;

  uint64_t iteration;
  RuntimeStatistics runtime_stats;
  InputStatistics input_stats;
  OutputStatistics output_stats;

  void first_pass()  {
    Query<T> *query;
    uint64_t key;
    access_t type;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    uint64_t data_id = size;
    for (uint64_t i = 0u; i < size; i++) {
      input_stats.num_txn_nodes++;
      query = queryBatch[i];
      iterator->setQuery(query);
      while (iterator->getNextAccess(key, type)) {
        auto info = &_info_array[key];
        if (info->epoch != iteration) {
          input_stats.num_data_nodes++;
          info->id = data_id++;
          info->epoch = iteration;
          info->num_reads = 0;
          info->num_writes = 0;
          info->txns.clear();
          info->cores.clear();
        }

        if (type == RD) {
          info->num_reads++;
        } else {
          info->num_writes++;
        }

        input_stats.num_edges++;
      }
    }
  }
  void second_pass() {
    uint64_t num_nodes = input_stats.num_txn_nodes + input_stats.num_data_nodes;
    xadj.reserve(num_nodes + 1);
    vwgt.reserve(num_nodes);
    adjncy.reserve(2 * input_stats.num_edges);
    adjwgt.reserve(2 * input_stats.num_edges);

    Query<T> *query;
    uint64_t key;
    access_t type;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();
    idx_t node_wgt = 0;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->setQuery(query);

      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      node_wgt = 0;
      while (iterator->getNextAccess(key, type)) {
        auto info = &_info_array[key];
        if (info->txns.empty()) {
          info->txns.reserve(info->num_writes + info->num_reads);
        }
        info->txns.push_back(i);
        adjncy.push_back(info->id);
        adjwgt.push_back(1);
      }
      vwgt.push_back(node_wgt);
    }
  }
  void third_pass()  {
    Query<T> *query;
    uint64_t key;
    access_t type;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();
    idx_t next_data_id = size;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->setQuery(query);
      while (iterator->getNextAccess(key, type)) {
        auto info = &_info_array[key];
        if (info->id == next_data_id) {
          xadj.push_back(static_cast<idx_t>(adjncy.size()));
          vwgt.push_back(0);
          adjncy.insert(adjncy.end(), info->txns.begin(), info->txns.end());
          adjwgt.insert(adjwgt.end(), info->txns.size(), 1);
          next_data_id++;
        }
      }
    }

    //final
    xadj.push_back(static_cast<idx_t>(adjncy.size()));
  }
  void compute_partition_stats(idx_t *parts, ClusterStatistics &stats) {
    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    Query<T> *query;
    uint64_t key;
    access_t type;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->setQuery(query);
      uint64_t cross_access = 0;
      while (iterator->getNextAccess(key, type)) {
        auto info = &_info_array[key];
        if (parts[i] != parts[info->id]) {
          cross_access++;
          if (info->cores.find(parts[i]) != info->cores.end()) {
            info->cores.insert(parts[i]);
          }
        }
      }
      stats.total_cross_access += cross_access;
      stats.min_txn_cross_access = min(cross_access, stats.min_txn_cross_access);
      stats.max_txn_cross_access = max(cross_access, stats.max_txn_cross_access);
    }

    idx_t next_data_id = size;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->setQuery(query);
      while (iterator->getNextAccess(key, type)) {
        auto info = &_info_array[key];
        if (info->id == next_data_id) {
          stats.min_data_core_degree =
                  min(static_cast<uint64_t>(info->cores.size()),
                      stats.min_data_core_degree);
          stats.max_data_core_degree =
                  max(static_cast<uint64_t>(info->cores.size()),
                      stats.max_data_core_degree);
          info->cores.clear();
          next_data_id++;
        }
      }
    }
  }
};

template <typename T>
class ConflictGraphPartitioner : public BasePartitioner<T> {};

#endif // DBX1000_PARTITIONER_H
