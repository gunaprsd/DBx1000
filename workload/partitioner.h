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
  void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) {
    _batch = batch;
    _iteration++;

    uint64_t start_time, end_time;
    start_time = get_server_clock();
    compute_graph_info();
    end_time = get_server_clock();
    _runtime_info.compute_graph_info_duration = DURATION(end_time, start_time);

    printf("************** Input Graph Information *************\n");
    _graph_info.print();

    auto parts = new idx_t[_graph_info.num_txn_nodes];
    for (uint32_t i = 0; i < _graph_info.num_txn_nodes; i++) {
      parts[i] = -1;
    }

    init_random(parts);
    compute_cluster_info(parts);
    printf("************** Random Cluster Information *************\n");
    _cluster_info.print();

    do_partition(parts);

    compute_cluster_info(parts, true);
    printf("************** Final Cluster Information *************\n");
    _cluster_info.print();

    printf("************** Runtime Information *************\n");
    _runtime_info.print();

    partitions.reserve(_graph_info.num_txn_nodes);
    for (uint64_t i = 0; i < _graph_info.num_txn_nodes; i++) {
      partitions.push_back(parts[i]);
    }
    delete parts;
  }

protected:
  uint32_t _num_clusters;
  uint32_t _iteration;
  QueryBatch<T> *_batch;
  GraphInfo<T> _graph_info;
  ClusterInfo<T> _cluster_info;
  RuntimeInfo _runtime_info;
  RandomNumberGenerator _rand;

  BasePartitioner(uint32_t num_clusters)
      : _num_clusters(num_clusters), _iteration(0), _batch(nullptr),
        _graph_info(), _cluster_info(), _rand(1) {
    _rand.seed(0, FLAGS_seed + 125);
  }

  void compute_graph_info() {
    _graph_info.reset();
    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    for (uint64_t i = 0u; i < size; i++) {
      _graph_info.num_txn_nodes++;
      query = queryBatch[i];
      uint64_t txn_degree = 0;

      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_graph_info.data_info[key];
        if (info->epoch != _iteration) {
          idx_t data_id = _graph_info.num_data_nodes + size;
          info->reset(data_id, _iteration, table_id);
          _graph_info.data_inv_idx.push_back(key);
          _graph_info.num_data_nodes++;
        }
        if (type == RD) {
          info->read_txns.push_back(i);
        } else {
          info->write_txns.push_back(i);
        }
        txn_degree++;
      }

      _graph_info.num_edges += txn_degree;
      ACCUMULATE_MIN(_graph_info.min_txn_degree, txn_degree);
      ACCUMULATE_MAX(_graph_info.max_txn_degree, txn_degree);
    }

    for (auto i = 0; i < _graph_info.data_inv_idx.size(); i++) {
      auto info = &_graph_info.data_info[_graph_info.data_inv_idx[i]];
      auto data_degree = info->read_txns.size() + info->write_txns.size();
      ACCUMULATE_MIN(_graph_info.min_data_degree, data_degree);
      ACCUMULATE_MAX(_graph_info.max_data_degree, data_degree);
    }
  }

  void init_random(idx_t *parts) {
    for (uint64_t i = 0; i < _graph_info.num_txn_nodes; i++) {
      parts[i] = _rand.nextInt64(0) % _num_clusters;
    }
  }

  void compute_cluster_info(idx_t *parts, bool select_cc = false) {
    _cluster_info.reset();

    _cluster_info.objective = 0;
    uint64_t *core_weights = new uint64_t[_num_clusters];
    for (auto i = 0; i < _graph_info.data_inv_idx.size(); i++) {
      uint64_t sum_c_sq = 0, sum_c = 0, num_c = 0, max_c = 0,
               chosen_c = UINT64_MAX;
      memset(core_weights, 0, sizeof(uint64_t) * _num_clusters);

      // compute core weights
      auto info = &_graph_info.data_info[_graph_info.data_inv_idx[i]];
      for (auto txn_id : info->read_txns) {
        core_weights[parts[txn_id]]++;
      }
      for (auto txn_id : info->write_txns) {
        core_weights[parts[txn_id]]++;
      }

      // compute stats on core weights
      for (uint64_t c = 0; c < _num_clusters; c++) {
        sum_c_sq += core_weights[c] ^ 2;
        sum_c += core_weights[c];
        num_c += core_weights[c] > 0 ? 1 : 0;
        if (core_weights[c] > max_c) {
          max_c = core_weights[c];
          chosen_c = c;
        }
      }

      // update table and objective info
      _cluster_info.objective += (sum_c ^ 2 - sum_c_sq) / 2;

      info->single_core = (num_c == 1);
      info->assigned_core = chosen_c;

      // update table wise info
      _cluster_info.table_info[info->table_id].num_accessed_data++;
      _cluster_info.table_info[info->table_id].num_total_accesses += sum_c;
      _cluster_info.table_info[info->table_id]
          .data_core_degree_histogram[num_c - 1]++;
    }

    auto iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    auto num_tables = AccessIterator<T>::get_num_tables();
    auto table_cross_access = new uint64_t[num_tables];
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      memset(table_cross_access, 0, sizeof(uint64_t) * num_tables);
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_graph_info.data_info[key];
        if (parts[i] != info->assigned_core) {
          table_cross_access[table_id]++;
        }
      }

      for (uint64_t s = 0; s < num_tables; s++) {
        uint64_t count = table_cross_access[s];
        _cluster_info.table_info[s].txn_cross_access_histogram[count - 1]++;
      }
    }
  }

  virtual void do_partition(idx_t *parts) = 0;
};

template <typename T> class AccessGraphPartitioner : public BasePartitioner<T> {

public:
  AccessGraphPartitioner(uint32_t num_clusters)
      : BasePartitioner<T>(num_clusters), vwgt(), adjwgt(), xadj(), adjncy(),
        vsize() {}

protected:
  vector<idx_t> vwgt;
  vector<idx_t> adjwgt;
  vector<idx_t> xadj;
  vector<idx_t> adjncy;
  vector<idx_t> vsize;

  void add_txn_nodes() {
    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;
    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      idx_t node_wgt = 0;
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_graph_info.data_info[key];

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

  void add_data_nodes() {
    for (auto i = 0; i < _graph_info.data_inv_idx.size(); i++) {
      auto info = &_graph_info.data_info[_graph_info.data_inv_idx[i]];
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
    }
  }

  void do_partition(idx_t *parts) override {
    auto start_time = get_server_clock();
    auto total_num_vertices =
        _graph_info.num_txn_nodes + _graph_info.num_data_nodes;
    xadj.reserve(total_num_vertices + 1);
    vwgt.reserve(total_num_vertices);
    vsize.reserve(total_num_vertices);
    adjncy.reserve(2 * _graph_info.num_edges);
    adjwgt.reserve(2 * _graph_info.num_edges);
    xadj.push_back(0);

    add_txn_nodes();

    add_data_nodes();

    auto all_parts = new idx_t[total_num_vertices];
    auto graph = new METIS_CSRGraph();
    graph->nvtxs = total_num_vertices;
    graph->adjncy_size = 2 * _graph_info.num_edges;
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->vsize = vsize.data();
    graph->ncon = 1;
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, all_parts);
    memcpy(parts, all_parts, sizeof(idx_t) * _graph_info.num_txn_nodes);

    auto end_time = get_server_clock();
    _runtime_info.partition_duration = DURATION(end_time, start_time);

    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();
    vsize.clear();
    delete all_parts;

  }
};

template <typename T> class ApproximateGraphPartitioner : public BasePartitioner<T> {

public:
    ApproximateGraphPartitioner(uint32_t num_clusters)
            : BasePartitioner<T>(num_clusters) {}

    void do_partition(idx_t* parts) {
      uint64_t start_time, end_time;
      for (uint32_t i = 0; i < FLAGS_iterations; i++) {
        start_time = get_server_clock();
        // partition data based on transaction allocation
        internal_data_partition(parts);
        // partition txn based on data allocation.
        internal_txn_partition(parts);
        end_time = get_server_clock();

        compute_cluster_info(parts);
        printf("**************** Iteration: %u ****************\n", i + 1);
        _cluster_info.print();
      }
    }
protected:

    void internal_txn_partition(idx_t *parts) {
      auto _cluster_size = new uint64_t[_num_clusters];
      memset(_cluster_size, 0, sizeof(uint64_t) * _num_clusters);
      Query<T> *query;
      uint64_t key;
      access_t type;
      uint32_t table_id;

      double max_cluster_size = ((1000 + FLAGS_ufactor) * input_stats.num_edges) /
                                (_num_clusters * 1000.0);
      // double max_cluster_size = UINT64_MAX;

      AccessIterator<T> *iterator = new AccessIterator<T>();
      QueryBatch<T> &queryBatch = *_batch;
      uint64_t size = queryBatch.size();

      uint64_t *savings = new uint64_t[_num_clusters];
      uint64_t *sorted = new uint64_t[_num_clusters];

      for (auto i = 0u; i < size; i++) {
        memset(savings, 0, sizeof(uint64_t) * _num_clusters);

        query = queryBatch[i];
        uint64_t txn_size = 0;
        iterator->set_query(query);
        while (iterator->next(key, type, table_id)) {
          auto info = &_data_info[key];
          auto core = parts[info->id];
          savings[core] += info->read_txns.size() + info->write_txns.size();
          txn_size++;
        }

        sort_helper(sorted, savings, _num_clusters);

        bool allotted = false;
        for (uint64_t s = 0; s < _num_clusters; s++) {
          auto core = sorted[s];
          if (_cluster_size[core] + txn_size < max_cluster_size) {
            assert(core >= 0 && core < _num_clusters);
            parts[i] = core;
            _cluster_size[core] += txn_size;
            allotted = true;
            break;
          }
        }
        assert(allotted);
      }

      delete[] sorted;
      delete[] savings;
    }

    void internal_data_partition(idx_t *parts) {
      uint64_t* core_weights = new uint64_t[_num_clusters];
      for(auto i = 0; i < data_inv_idx.size(); i++) {
        auto info = &_data_info[data_inv_idx[i]];
        memset(core_weights, 0, sizeof(uint64_t) * _num_clusters);
        for(auto txn_id : info->read_txns) {
          core_weights[parts[txn_id]]++;
        }
        for(auto txn_id: info->write_txns) {
          core_weights[parts[txn_id]]++;
        }
        uint64_t max_value = 0;
        uint64_t allotted_core = 0;
        for (uint64_t c = 0; c < _num_clusters; c++) {
          if (core_weights[c] > max_value) {
            max_value = core_weights[c];
            allotted_core = c;
          }
        }
        parts[info->id] = allotted_core;
      }
    }

    void sort_helper(uint64_t *index, uint64_t *value, uint64_t size) {
      for (uint64_t i = 0; i < size; i++) {
        index[i] = i;
      }

      for (uint64_t i = 1; i < size; i++) {
        for (uint64_t j = 0; j < size - i; j++) {
          if (value[index[j + 1]] > value[index[j]]) {
            auto temp = index[j + 1];
            index[j + 1] = index[j];
            index[j] = temp;
          }
        }
      }

      for (uint64_t i = 0; i < size - 1; i++) {
        assert(value[index[i]] >= value[index[i + 1]]);
      }
    }
};

#endif // DBX1000_PARTITIONER_H
