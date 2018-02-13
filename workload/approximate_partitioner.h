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
  DataNodeInfo *_data_info;
  TableInfo *_table_info;
  vector<uint64_t> data_inv_idx;
  uint64_t *_cluster_size;
  uint64_t iteration;
  RuntimeStatistics runtime_stats;
  InputStatistics input_stats;
  OutputStatistics output_stats;

public:
  ApproximateGraphPartitioner(uint32_t num_clusters)
      : _num_clusters(num_clusters), _batch(nullptr), _data_info(nullptr),
        _table_info(nullptr), data_inv_idx(), _cluster_size(nullptr),
        iteration(0), runtime_stats(), input_stats(), output_stats() {
    // Data information
    uint64_t size = AccessIterator<T>::get_max_key();
    _data_info = new DataNodeInfo[size];

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

    compute_partition_stats(parts, output_stats.output_cluster);
    printf("**************** Iteration: 0 ****************\n");
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

      compute_partition_stats(parts, output_stats.output_cluster);
      printf("**************** Iteration: %u ****************\n", i + 1);
      print_partition_stats();
    }

    internal_data_partition(parts);
    compute_partition_stats(parts, output_stats.output_cluster, true);
    printf("**************** Final Cluster ****************\n");
    print_partition_stats();

    printf("************** Runtime Information *************\n");
    runtime_stats.print();

    partitions.reserve(input_stats.num_txn_nodes);
    for (uint64_t i = 0; i < input_stats.num_txn_nodes; i++) {
      partitions.push_back(parts[i]);
    }

    delete[] parts;
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
      iterator->set_query(query);
      uint64_t txn_degree = 0;
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

    for(auto i = 0; i < data_inv_idx.size(); i++) {
			auto info = &_data_info[data_inv_idx[i]];
			auto data_degree = info->read_txns.size() + info->write_txns.size();
			ACCUMULATE_MIN(input_stats.min_data_degree, data_degree);
			ACCUMULATE_MAX(input_stats.max_data_degree, data_degree);
		}
  }

  void internal_txn_partition(idx_t *parts) {
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
};

#endif // DBX1000_PARTITIONER_H
