#ifndef DBX1000_PARTITIONER_H
#define DBX1000_PARTITIONER_H
#include "distributions.h"
#include "global.h"
#include "graph_partitioner.h"
#include "partitioner_helper.h"
#include "query.h"
#include <cstring>

template <typename T> class BasePartitioner {
public:
  virtual void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) = 0;
  virtual void print_stats() = 0;
};

template <typename T> class AccessGraphPartitioner : public BasePartitioner<T> {
  struct RuntimeStatistics {
    double init_pass_duration;
    double second_pass_duration;
    double third_pass_duration;
    double partition_duration;

    RuntimeStatistics() { reset(); }

    void reset() {
      init_pass_duration = 0;
      second_pass_duration = 0;
      third_pass_duration = 0;
      partition_duration = 0;
    }

    void print() {
      PRINT_INFO(-10lf, "First-Pass-Duration", init_pass_duration);
      PRINT_INFO(-10lf, "Second-Pass-Duration", second_pass_duration);
      PRINT_INFO(-10lf, "Third-Pass-Duration", third_pass_duration);
      PRINT_INFO(-10lf, "Partition-Duration", partition_duration);
    }
  };

public:
  AccessGraphPartitioner(uint32_t num_clusters)
      : _num_clusters(num_clusters), _batch(nullptr), _data_info(nullptr),
        _table_info(nullptr), data_inv_idx(), vwgt(), adjwgt(), xadj(),
        adjncy(), vsize(), iteration(0), runtime_stats(), input_stats(),
        output_stats() {
    uint64_t size = AccessIterator<T>::get_max_key();
    _data_info = new DataNodeInfo[size];
    auto num_tables = AccessIterator<T>::get_num_tables();
    _table_info = new TableInfo[num_tables];
    for (uint64_t i = 0; i < num_tables; i++) {
      _table_info[i].initialize(num_clusters);
    }
  }

  void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) override {
    _batch = batch;
    iteration++;
    // reset all parameters

    uint64_t start_time, end_time;
    start_time = get_server_clock();
    init_pass();
    end_time = get_server_clock();
    runtime_stats.init_pass_duration = DURATION(end_time, start_time);

    printf("************** Input Information *************\n");
    input_stats.print();

    // Initialization
    uint64_t total_num_vertices =
        input_stats.num_txn_nodes + input_stats.num_data_nodes;
    auto parts = new idx_t[total_num_vertices];
    for (uint32_t i = 0; i < total_num_vertices; i++) {
      parts[i] = -1;
    }

    init_random(parts);
    compute_partition_stats(parts, output_stats.output_cluster);
    printf("**************** Random Clusters ****************\n");
    print_partition_stats();

    start_time = get_server_clock();
    auto graph = new METIS_CSRGraph();
    graph->nvtxs = total_num_vertices;
    graph->adjncy_size = 2 * input_stats.num_edges;
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->vsize = vsize.data();
    graph->ncon = 1;
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, parts);
    end_time = get_server_clock();
    runtime_stats.partition_duration = DURATION(end_time, start_time);

    compute_partition_stats(parts, output_stats.output_cluster, true);
    printf("**************** METIS Clusters ****************\n");
    print_partition_stats();

    printf("************** Runtime Information *************\n");
    runtime_stats.print();

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
    vsize.clear();
    data_inv_idx.clear();
  }

protected:
  const uint32_t _num_clusters;
  QueryBatch<T> *_batch;
  DataNodeInfo *_data_info;
  TableInfo *_table_info;
  vector<uint64_t> data_inv_idx;
  vector<idx_t> vwgt;
  vector<idx_t> adjwgt;
  vector<idx_t> xadj;
  vector<idx_t> adjncy;
  vector<idx_t> vsize;

  uint64_t iteration;
  RuntimeStatistics runtime_stats;
  InputStatistics input_stats;
  OutputStatistics output_stats;

  void init_pass() {
    first_pass();
    second_pass();
    third_pass();
  }

  void first_pass() {
    input_stats.reset();

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    for (uint64_t i = 0u; i < size; i++) {
      input_stats.num_txn_nodes++;
      query = queryBatch[i];
      uint64_t txn_degree = 0;

      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->epoch != iteration) {
          idx_t data_id = input_stats.num_data_nodes + size;
          info->reset(data_id, iteration, table_id);
          data_inv_idx.push_back(key);
          input_stats.num_data_nodes++;
        }
        if (type == RD) {
          info->read_txns.push_back(i);
        } else {
          info->write_txns.push_back(i);
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

        idx_t wt = 1;
        if (!FLAGS_unit_weights) {
          if (type == RD) {
            wt += static_cast<idx_t>(info->write_txns.size());
          } else if (type == WR) {
            wt += static_cast<idx_t>(info->read_txns.size() +
                                    info->write_txns.size());
          }
        }

        adjncy.push_back(info->id);
        adjwgt.push_back(wt);
        node_wgt++;
      }

      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(node_wgt);
      vsize.push_back(0);
    }
  }

  void third_pass() {
    for (auto i = 0; i < data_inv_idx.size(); i++) {
      auto info = &_data_info[data_inv_idx[i]];
      // insert txn edges
      adjncy.insert(adjncy.end(), info->read_txns.begin(),
                    info->read_txns.end());
      adjncy.insert(adjncy.end(), info->write_txns.begin(),
                    info->write_txns.end());

      // insert edge weights
      idx_t read_wgt = 1, write_wgt = 1;
      if (!FLAGS_unit_weights) {
        read_wgt = static_cast<idx_t>(info->write_txns.size());
        write_wgt = static_cast<idx_t>(info->read_txns.size() +
                                info->write_txns.size());
      }

      adjwgt.insert(adjwgt.end(), info->read_txns.size(), read_wgt);
      adjwgt.insert(adjwgt.end(), info->write_txns.size(), write_wgt);

      // insert node details
      xadj.push_back(static_cast<idx_t>(adjncy.size()));
      vwgt.push_back(0);
      auto data_degree = info->read_txns.size() + info->write_txns.size();
      if (FLAGS_unit_weights) {
        vsize.push_back(1);
      } else {
        vsize.push_back(static_cast<idx_t>(data_degree));
      }
      ACCUMULATE_MIN(input_stats.min_data_degree, data_degree);
      ACCUMULATE_MAX(input_stats.max_data_degree, data_degree);
    }
  }

  void compute_partition_stats(idx_t *parts, ClusterStatistics &stats,
                               bool select_cc = false) {
    stats.reset();

    stats.objective = 0;
    uint64_t* core_weights = new uint64_t[_num_clusters];
    for (auto i = 0; i < data_inv_idx.size(); i++) {
      uint64_t sum_c_sq = 0, sum_c = 0, num_c = 0, max_c = 0, chosen_c = UINT64_MAX;
      memset(core_weights, 0, sizeof(uint64_t) * _num_clusters);

      //compute core weights
      auto info = &_data_info[data_inv_idx[i]];
      for(auto txn_id : info->read_txns) {
        core_weights[parts[txn_id]]++;
      }
      for(auto txn_id: info->write_txns) {
        core_weights[parts[txn_id]]++;
      }

      //compute stats on core weights
      for (uint64_t c = 0; c < _num_clusters; c++) {
        sum_c_sq += core_weights[c]^2;
        sum_c += core_weights[c];
        num_c += core_weights[c] > 0 ? 1 : 0;
        if(core_weights[c] > max_c) {
          max_c = core_weights[c];
          chosen_c = c;
        }
      }

      //update table and objective info
      stats.objective += (sum_c^2 - sum_c_sq)/2;

      info->single_core = (num_c == 1);
      info->assigned_core = chosen_c;

      //update table wise info
      _table_info[info->table_id].num_accessed_data++;
      _table_info[info->table_id].num_total_accesses += sum_c;
      _table_info[info->table_id].num_cross_accesses += sum_c - max_c;
      _table_info[info->table_id].core_distribution[num_c - 1]++;
    }

    auto iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];

      uint64_t cross_access_read = 0, cross_access_write = 0;
      //compute cross_access_read and cross_access_write
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (parts[i] != info->assigned_core) {
          if (type == RD) {
            cross_access_read++;
          } else if (type == WR) {
            cross_access_write++;
          }
        }
      }

      ACCUMULATE_SUM(stats.tot_cross_access_read, cross_access_read);
      ACCUMULATE_MIN(stats.min_cross_access_read, cross_access_read);
      ACCUMULATE_MAX(stats.max_cross_access_read, cross_access_read);

      ACCUMULATE_SUM(stats.tot_cross_access_write, cross_access_write);
      ACCUMULATE_MIN(stats.min_cross_access_write, cross_access_write);
      ACCUMULATE_MAX(stats.max_cross_access_write, cross_access_write);
    }
  }

  void init_random(idx_t *parts) {
    uint64_t total_num_vertices =
        input_stats.num_txn_nodes + input_stats.num_data_nodes;
    RandomNumberGenerator randomNumberGenerator(1);
    randomNumberGenerator.seed(0, FLAGS_seed + 125);
    for (size_t i = 0; i < total_num_vertices; i++) {
      parts[i] = randomNumberGenerator.nextInt64(0) % _num_clusters;
    }
  }

  void print_partition_stats() {
    output_stats.output_cluster.print();
    auto num_tables = AccessIterator<T>::get_num_tables();
    for (uint32_t i = 0; i < num_tables; i++) {
      _table_info[i].print(get_table_name<T>(i));
    }
  }
};

template <typename T>
class ConflictGraphPartitioner : public BasePartitioner<T> {};

#endif // DBX1000_PARTITIONER_H
