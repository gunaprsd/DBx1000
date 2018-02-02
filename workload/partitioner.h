#ifndef DBX1000_PARTITIONER_H
#define DBX1000_PARTITIONER_H
#include "distributions.h"
#include "global.h"
#include "graph_partitioner.h"
#include "partitioned_helper.h"
#include "query.h"

template <typename T> class BasePartitioner {
public:
  virtual void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) = 0;
};

template <typename T> class AccessGraphPartitioner : public BasePartitioner<T> {

public:
  AccessGraphPartitioner(uint32_t num_clusters)
      : _num_clusters(num_clusters), _batch(nullptr), _data_info(nullptr),
        _table_info(nullptr), vwgt(), adjwgt(), xadj(), adjncy(), vsize(),
        iteration(0), runtime_stats(), input_stats(), output_stats() {
    uint64_t size = AccessIterator<T>::get_max_key();
    _data_info = new DataNodeInfo[size];
    auto num_tables = AccessIterator<T>::get_num_tables();
    _table_info = new TableInfo[num_tables];
    for (uint64_t i = 0; i < num_tables; i++) {
      _table_info[i].reset();
    }
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
    printf("First pass completed\n");

    start_time = get_server_clock();
    second_pass();
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.second_pass_duration = duration;
    printf("Second pass completed\n");

    start_time = get_server_clock();
    third_pass();
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.third_pass_duration = duration;
    printf("Third pass completed\n");

    auto graph = new METIS_CSRGraph();
    uint64_t total_num_vertices =
        input_stats.num_txn_nodes + input_stats.num_data_nodes;
    graph->nvtxs = total_num_vertices;
    graph->adjncy_size = 2 * input_stats.num_edges;
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->vsize = vsize.data();
    graph->ncon = 1;

    // Initialization
    auto parts = new idx_t[total_num_vertices];
    for (uint32_t i = 0; i < total_num_vertices; i++) {
      parts[i] = -1;
    }

    compute_baseline_stats(parts, output_stats.random_cluster);
    printf("Baseline stats computed\n");

    start_time = get_server_clock();
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, parts);
    end_time = get_server_clock();
    duration = DURATION(end_time, start_time);
    runtime_stats.partition_duration = duration;
    printf("Clustering completed\n");

    compute_partition_stats(parts, output_stats.output_cluster);
    printf("Output stats computed\n");

    // Add resulting clustering into provided vector
    partitions.reserve(input_stats.num_txn_nodes);
    for (uint64_t i = 0; i < input_stats.num_txn_nodes; i++) {
      partitions.push_back(parts[i]);
    }

    delete graph;
    delete[] parts;
    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();
  }

  void print_stats() {
    PRINT_INFO(lu, "Iteration", iteration);
    input_stats.print();
    runtime_stats.print();
    output_stats.print();
    auto num_tables = AccessIterator<T>::get_num_tables();
    for (uint32_t i = 0; i < num_tables; i++) {
      _table_info[i].print(get_table_name<T>(i));
    }
  }

protected:
  const uint32_t _num_clusters;
  QueryBatch<T> *_batch;
  DataNodeInfo *_data_info;
  TableInfo *_table_info;
  vector<idx_t> vwgt;
  vector<idx_t> adjwgt;
  vector<idx_t> xadj;
  vector<idx_t> adjncy;
  vector<idx_t> vsize;

  uint64_t iteration;
  RuntimeStatistics runtime_stats;
  InputStatistics input_stats;
  OutputStatistics output_stats;

  void first_pass() {
    input_stats.num_txn_nodes = 0;
    input_stats.num_data_nodes = 0;
    input_stats.num_edges = 0;
    input_stats.min_txn_degree = UINT64_MAX;
    input_stats.max_txn_degree = 0;

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    uint64_t data_id = size;
    for (uint64_t i = 0u; i < size; i++) {
      input_stats.num_txn_nodes++;
      query = queryBatch[i];
      iterator->set_query(query);
      uint64_t txn_degree = 0;
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->epoch != iteration) {
          input_stats.num_data_nodes++;
          info->id = data_id++;
          info->epoch = iteration;
          info->num_reads = 0;
          info->num_writes = 0;
          info->read_txns.clear();
          info->cores.clear();
        }

        if (type == RD) {
          info->num_reads++;
        } else {
          info->num_writes++;
        }

        txn_degree++;
      }
      input_stats.num_edges += txn_degree;
      ACCUMULATE_MIN(input_stats.min_txn_degree, txn_degree);
      ACCUMULATE_MAX(input_stats.max_txn_degree, txn_degree);
    }
  }

  void second_pass() {
    input_stats.min_data_degree = UINT64_MAX;
    input_stats.max_data_degree = 0;

    uint64_t num_nodes = input_stats.num_txn_nodes + input_stats.num_data_nodes;
    xadj.reserve(num_nodes + 1);
    vwgt.reserve(num_nodes);
    vsize.reserve(num_nodes);
    adjncy.reserve(2 * input_stats.num_edges);
    adjwgt.reserve(2 * input_stats.num_edges);

    xadj.push_back(0);

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);

      idx_t node_wgt = 0;
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];

        if (info->read_txns.empty() && info->write_txns.empty()) {
          info->read_txns.reserve(info->num_reads);
          info->write_txns.reserve(info->num_writes);
        }

        if (type == RD) {
          info->read_txns.push_back(i);
        } else if (type == WR) {
          info->write_txns.push_back(i);
        }

        adjncy.push_back(info->id);
        adjwgt.push_back(1);
        node_wgt++;
      }

      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(node_wgt);
      vsize.push_back(0);
    }
  }

  void third_pass() {
    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    idx_t next_data_id = size;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);

      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->id == next_data_id) {
          // insert txn edges
          adjncy.insert(adjncy.end(), info->read_txns.begin(),
                        info->read_txns.end());
          adjncy.insert(adjncy.end(), info->write_txns.begin(),
                        info->write_txns.end());

          // insert edge weights
          adjwgt.insert(adjwgt.end(), info->read_txns.size(), 1);
          adjwgt.insert(adjwgt.end(), info->write_txns.size(), 1);

          // insert node details
          xadj.push_back(static_cast<idx_t>(adjncy.size()));
          vwgt.push_back(0);
          vsize.push_back(1);

          auto data_degree = info->num_reads + info->num_writes;
          ACCUMULATE_MIN(input_stats.min_data_degree, data_degree);
          ACCUMULATE_MAX(input_stats.max_data_degree, data_degree);
          next_data_id++;
        }
      }
    }
  }

  void compute_baseline_stats(idx_t *parts, ClusterStatistics &stats) {
    // Computing a baseline allotment
    uint64_t total_num_vertices =
        input_stats.num_txn_nodes + input_stats.num_data_nodes;
    RandomNumberGenerator randomNumberGenerator(1);
    randomNumberGenerator.seed(0, FLAGS_seed + 125);
    for (size_t i = 0; i < total_num_vertices; i++) {
      parts[i] = randomNumberGenerator.nextInt64(0) % _num_clusters;
    }

    stats.tot_cross_access_read = 0;
    stats.min_cross_access_read = UINT64_MAX;
    stats.max_cross_access_read = 0;
    stats.min_data_core_degree = UINT64_MAX;
    stats.max_data_core_degree = 0;
    stats.tot_cross_access_write = 0;
    stats.min_cross_access_write = UINT64_MAX;
    stats.max_cross_access_write = 0;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;
    // This pass lets you initiate the cores set for each data item
    // Also, it computes min and max cross access for each transaction
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);
      uint64_t cross_access_read = 0;
      uint64_t cross_access_write = 0;
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (parts[i] != parts[info->id]) {
          if (type == RD) {
            cross_access_read++;
          } else if (type == WR) {
            cross_access_write++;
          }
        }
        if (info->cores.find(parts[i]) == info->cores.end()) {
          info->cores.insert(parts[i]);
        }
      }

      ACCUMULATE_SUM(stats.tot_cross_access_read, cross_access_read);
      ACCUMULATE_MIN(stats.min_cross_access_read, cross_access_read);
      ACCUMULATE_MAX(stats.max_cross_access_read, cross_access_read);

      ACCUMULATE_SUM(stats.tot_cross_access_write, cross_access_write);
      ACCUMULATE_MIN(stats.min_cross_access_write, cross_access_write);
      ACCUMULATE_MAX(stats.max_cross_access_write, cross_access_write);
    }

    // In this pass, we compute the min and max core degree for
    // each data item
    idx_t next_data_id = size;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->id == next_data_id) {
          auto num_cores = info->cores.size();
          ACCUMULATE_MIN(stats.min_data_core_degree, num_cores);
          ACCUMULATE_MAX(stats.max_data_core_degree, num_cores);
          if (num_cores == 1) {
            stats.num_single_core_data++;
          }
          info->cores.clear();
          next_data_id++;
        }
      }
    }
  }

  void compute_partition_stats(idx_t *parts, ClusterStatistics &stats) {
    stats.tot_cross_access_read = 0;
    stats.min_cross_access_read = UINT64_MAX;
    stats.max_cross_access_read = 0;
    stats.min_data_core_degree = UINT64_MAX;
    stats.max_data_core_degree = 0;
    stats.tot_cross_access_write = 0;
    stats.min_cross_access_write = UINT64_MAX;
    stats.max_cross_access_write = 0;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);
      uint64_t cross_access_read = 0;
      uint64_t cross_access_write = 0;
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (parts[i] != parts[info->id]) {
          if (type == RD) {
            cross_access_read++;
          } else if (type == WR) {
            cross_access_write++;
          }
          _table_info[table_id].num_cross_accesses++;
        }
        if (info->cores.find(parts[i]) == info->cores.end()) {
          info->cores.insert(parts[i]);
        }
        _table_info[table_id].num_total_accesses++;
      }

      ACCUMULATE_SUM(stats.tot_cross_access_read, cross_access_read);
      ACCUMULATE_MIN(stats.min_cross_access_read, cross_access_read);
      ACCUMULATE_MAX(stats.max_cross_access_read, cross_access_read);

      ACCUMULATE_SUM(stats.tot_cross_access_write, cross_access_write);
      ACCUMULATE_MIN(stats.min_cross_access_write, cross_access_write);
      ACCUMULATE_MAX(stats.max_cross_access_write, cross_access_write);
    }

    idx_t next_data_id = size;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->id == next_data_id) {
          auto num_cores = info->cores.size();
          ACCUMULATE_MIN(stats.min_data_core_degree, num_cores);
          ACCUMULATE_MAX(stats.max_data_core_degree, num_cores);
          ACCUMULATE_MAX(_table_info[table_id].max_data_core_degree, num_cores);
          if (num_cores == 1) {
            stats.num_single_core_data++;
            _table_info[table_id].num_single_core_data++;
          }

#ifdef SELECTIVE_CC
          if (info->cores.empty()) {
            iterator->set_cc_info(0);
          } else {
            iterator->set_cc_info(1);
          }
#endif
          next_data_id++;
          _table_info[table_id].num_accessed_data++;
        }
      }
    }
  }
};

template <typename T>
class ConflictGraphPartitioner : public BasePartitioner<T> {};

#endif // DBX1000_PARTITIONER_H
