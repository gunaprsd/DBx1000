#ifndef DBX1000_APPROX_PARTITIONER_H
#define DBX1000_APPROX_PARTITIONER_H
#include "partitioner.h"
#include "partitioner_helper.h"

template <typename T>
class ApproximateGraphPartitioner : public BasePartitioner<T> {
  struct RuntimeStatistics {
    double init_pass_duration;
    double partition_duration;
    uint32_t num_iterations;

    RuntimeStatistics() { reset(); }

    void reset() {
      init_pass_duration = 0;
      partition_duration = 0;
      num_iterations = 0;
    }

    void print() {
      PRINT_INFO(-10lf, "Init-Pass-Duration", init_pass_duration);
      PRINT_INFO(-10lf, "Partition-Duration", partition_duration);
      auto avg_duration = partition_duration / (double)num_iterations;
      PRINT_INFO(-10lf, "Avg-Partition-Duration", avg_duration);
    }
  };

protected:
  const uint32_t _num_clusters;
  QueryBatch<T> *_batch;
  ApproxDataNodeInfo *_data_info;
  TableInfo *_table_info;
  uint64_t *_cluster_size;

  uint64_t iteration;
  RuntimeStatistics runtime_stats;
  InputStatistics input_stats;
  OutputStatistics output_stats;

public:
  ApproximateGraphPartitioner(uint32_t num_clusters)
      : _num_clusters(num_clusters), _batch(nullptr), _data_info(nullptr),
        _table_info(nullptr), iteration(0), runtime_stats(), input_stats(),
        output_stats() {
    // Data information
    uint64_t size = AccessIterator<T>::get_max_key();
    _data_info = new ApproxDataNodeInfo[size];

    // For recording cluster information
    auto num_tables = AccessIterator<T>::get_num_tables();
    _table_info = new TableInfo[num_tables];
    for (uint64_t i = 0; i < num_tables; i++) {
      _table_info[i].initialize(num_clusters);
    }

    _cluster_size = new uint64_t[num_clusters];
  }

  void partition(QueryBatch<T> *batch, vector<idx_t> &partitions) override {
    _batch = batch;
    iteration++;

    uint64_t start_time, end_time;

    start_time = get_server_clock();
    init_pass();
    end_time = get_server_clock();
    runtime_stats.init_pass_duration = DURATION(end_time, start_time);

    printf("************** Input Information *************\n");
    input_stats.print();

    uint64_t total_num_vertices =
        input_stats.num_txn_nodes + input_stats.num_data_nodes;
    auto parts = new idx_t[total_num_vertices];
    init_random_partition(parts);

    printf("**************** Iteration: 0 ****************\n");
    compute_partition_stats(parts, output_stats.output_cluster);
    print_partition_stats();

    for (uint32_t i = 0; i < FLAGS_iterations; i++) {
      start_time = get_server_clock();

      // partition data based on transaction allocation
      internal_data_partition(parts);
      // partition txn based on data allocation.
      internal_txn_partition(parts);

      end_time = get_server_clock();
      runtime_stats.num_iterations++;
      runtime_stats.partition_duration += DURATION(end_time, start_time);

      printf("**************** Iteration: %u ****************\n", i + 1);
      compute_partition_stats(parts, output_stats.output_cluster);
      print_partition_stats();
    }

    printf("************** Runtime Information *************\n");
    runtime_stats.print();

    partitions.reserve(input_stats.num_txn_nodes);
    for (uint64_t i = 0; i < input_stats.num_txn_nodes; i++) {
      partitions.push_back(parts[i]);
    }

    delete[] parts;
  }

  void print_stats() {
    runtime_stats.print();
    output_stats.print();
    auto num_tables = AccessIterator<T>::get_num_tables();
    for (uint32_t i = 0; i < num_tables; i++) {
      _table_info[i].print(get_table_name<T>(i));
    }
  }

  void print_partition_stats() {
    output_stats.output_cluster.print();
    auto num_tables = AccessIterator<T>::get_num_tables();
    for (uint32_t i = 0; i < num_tables; i++) {
      _table_info[i].print(get_table_name<T>(i));
    }
  }

protected:
  void init_pass() {
    input_stats.num_txn_nodes = 0;
    input_stats.num_data_nodes = 0;
    input_stats.num_edges = 0;
    input_stats.min_txn_degree = UINT64_MAX;
    input_stats.max_txn_degree = 0;
    input_stats.min_data_degree = UINT64_MAX;
    input_stats.max_data_degree = 0;

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
          info->init(iteration, data_id++, _num_clusters);
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

    idx_t next_data_id = size;
    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->id == next_data_id) {
          auto data_degree = info->num_writes + info->num_reads;
          ACCUMULATE_MIN(input_stats.min_data_degree, data_degree);
          ACCUMULATE_MAX(input_stats.max_data_degree, data_degree);
          next_data_id++;
        }
      }
    }
  }

  void internal_txn_partition(idx_t *parts) {
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

    // empty init cluster sizes
    for (uint64_t s = 0; s < _num_clusters; s++) {
      _cluster_size[s] = 0;
    }

    for (auto i = 0u; i < size; i++) {
      query = queryBatch[i];
      // empty init savings array
      for (uint64_t s = 0; s < _num_clusters; s++) {
        savings[s] = 0;
      }

      // compute savings for each core
      uint64_t txn_size = 0;
      iterator->set_query(query);
      while (iterator->next(key, type, table_id)) {
        auto info = &_data_info[key];
        if (info->read_wgt == 0 || info->write_wgt == 0) {
          info->read_wgt = 1 + info->num_writes;
          info->write_wgt = 1 + info->num_reads + info->num_writes;
        }
        auto core = parts[info->id];
        // savings[core] += type == RD ? info->read_wgt : info->write_wgt;
        savings[core] += info->write_wgt;
        txn_size++;
      }

      // sort savings and store indices in sorted array
      sort_helper(sorted, savings, _num_clusters);

      bool allotted = false;
      for (uint64_t s = 0; s < _num_clusters; s++) {
        auto core = sorted[s];
        if (_cluster_size[core] + txn_size < max_cluster_size) {
          assert(core >= 0 && core < _num_clusters);
          parts[i] = core;
          _cluster_size[core] += txn_size;

          // Add core weight for each data item
          iterator->set_query(query);
          while (iterator->next(key, type, table_id)) {
            auto info = &_data_info[key];
            info->core_weights[core]++;
          }

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
          uint64_t max_value = 0;
          uint64_t allotted_core = 0;
          for (uint64_t c = 0; c < _num_clusters; c++) {
            if (info->core_weights[c] > max_value) {
              max_value = info->core_weights[c];
              allotted_core = c;
            }
          }
          parts[info->id] = allotted_core;
          next_data_id++;
        }
      }
    }
  }

  void init_random_partition(idx_t *parts) {
    uint64_t total_num_vertices =
        input_stats.num_txn_nodes + input_stats.num_data_nodes;
    RandomNumberGenerator randomNumberGenerator(1);
    randomNumberGenerator.seed(0, FLAGS_seed + 125);
    for (size_t i = 0; i < total_num_vertices; i++) {
      parts[i] = randomNumberGenerator.nextInt64(0) % _num_clusters;
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

  void compute_partition_stats(idx_t *parts, ClusterStatistics &stats) {
    stats.reset();
    for (uint32_t i = 0; i < AccessIterator<T>::get_num_tables(); i++) {
      _table_info[i].reset(_num_clusters);
    }

    AccessIterator<T> *iterator = new AccessIterator<T>();
    QueryBatch<T> &queryBatch = *_batch;
    uint64_t size = queryBatch.size();

    Query<T> *query;
    uint64_t key;
    access_t type;
    uint32_t table_id;

    // Computing transaction information
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

    // Computing data information
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
          _table_info[table_id].core_distribution[num_cores - 1]++;
          _table_info[table_id].num_accessed_data++;
          info->cores.clear();
          next_data_id++;
        }
      }
    }
  }
};

#endif // DBX1000_PARTITIONER_H
