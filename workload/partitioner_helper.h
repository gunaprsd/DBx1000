#ifndef DBX1000_PARTITIONED_HELPER_H
#define DBX1000_PARTITIONED_HELPER_H

#include "global.h"
#include <metis.h>

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
    PRINT_INFO(-10lf, "First-Pass-Duration", first_pass_duration);
    PRINT_INFO(-10lf, "Second-Pass-Duration", second_pass_duration);
    PRINT_INFO(-10lf, "Third-Pass-Duration", third_pass_duration);
    PRINT_INFO(-10lf, "Partition-Duration", partition_duration);
  }
};

struct TableInfo {
  uint64_t num_total_accesses;
  uint64_t num_cross_accesses;
  uint64_t num_accessed_data;
  uint64_t max_num_cores;
  uint64_t *core_distribution;

  TableInfo() { core_distribution = nullptr; }

  void initialize(uint64_t _max_num_cores) { reset(_max_num_cores); }

  void reset(uint64_t _max_num_cores) {
    num_total_accesses = 0;
    num_cross_accesses = 0;
    num_accessed_data = 0;

    if (core_distribution == nullptr) {
      this->max_num_cores = _max_num_cores;
      core_distribution = new uint64_t[max_num_cores];
    } else {
      assert(max_num_cores == _max_num_cores);
    }
    for (uint64_t i = 0; i < max_num_cores; i++) {
      core_distribution[i] = 0;
    }
  }

  void print(string name) {
    printf("%s-%-25s: %lu\n", name.c_str(), "Total-Accesses",
           num_total_accesses);
    printf("%s-%-25s: %lu\n", name.c_str(), "Cross-Accesses",
           num_cross_accesses);
    printf("%s-%-25s: %lu\n", name.c_str(), "Num-Accessed-Data",
           num_accessed_data);
    printf("%s-%-25s: [", name.c_str(), "Core-Distribution");
    double weighted_num_cores = 0.0;
    for (uint64_t i = 0; i < max_num_cores; i++) {
      printf("%lu, ", core_distribution[i]);
      weighted_num_cores += core_distribution[i] * (i + 1);
    }
    printf("]\n");
    double weighted_avg_num_cores =
        num_accessed_data > 0 ? weighted_num_cores / num_accessed_data : 0;
    printf("%s-%-25s: %lf\n", name.c_str(), "Weighted-Avg-Cores",
           weighted_avg_num_cores);
  }
};

struct DataNodeInfo {
  idx_t id;
  uint64_t epoch;
  uint64_t num_reads;
  uint64_t num_writes;
  idx_t read_wgt;
  idx_t write_wgt;
  std::set<idx_t> cores;
  std::vector<idx_t> read_txns;
  std::vector<idx_t> write_txns;
  DataNodeInfo()
      : id(-1), epoch(UINT64_MAX), num_reads(0), num_writes(0), cores(),
        read_txns(), write_txns() {}
};

struct ApproxDataNodeInfo {
  idx_t id;
  uint64_t epoch;
  uint64_t num_reads;
  uint64_t num_writes;
  idx_t read_wgt;
  idx_t write_wgt;
	uint64_t* core_weights;
  std::set<idx_t> cores;
  ApproxDataNodeInfo()
      : id(-1), epoch(UINT64_MAX), num_reads(0), num_writes(0), cores() {}
};

struct InputStatistics {
  uint64_t num_txn_nodes;
  uint64_t num_data_nodes;
  uint64_t num_edges;
  uint64_t min_data_degree;
  uint64_t max_data_degree;
  uint64_t min_txn_degree;
  uint64_t max_txn_degree;

  InputStatistics() { reset(); }

  void reset() {
    num_txn_nodes = 0;
    num_data_nodes = 0;
    num_edges = 0;
    min_data_degree = 0;
    max_data_degree = 0;
    min_txn_degree = 0;
    max_txn_degree = 0;
  }

  void print() {
    PRINT_INFO(lu, "Num-Data-Nodes", num_data_nodes);
    PRINT_INFO(lu, "Num-Txn-Nodes", num_txn_nodes);
    PRINT_INFO(lu, "Num-Edges", num_edges);
    PRINT_INFO(lu, "Min-Data-Degree", min_data_degree);
    PRINT_INFO(lu, "Max-Data-Degree", max_data_degree);
    PRINT_INFO(lu, "Min-Txn-Degree", min_txn_degree);
    PRINT_INFO(lu, "Max-Txn-Degree", max_txn_degree);
  }
};

struct ClusterStatistics {
  uint64_t num_single_core_data;
  uint64_t min_data_core_degree;
  uint64_t max_data_core_degree;

  uint64_t min_cross_access_read;
  uint64_t max_cross_access_read;
  uint64_t tot_cross_access_read;

  uint64_t min_cross_access_write;
  uint64_t max_cross_access_write;
  uint64_t tot_cross_access_write;

  ClusterStatistics() { reset(); }

  void reset() {
    num_single_core_data = 0;
    min_data_core_degree = 1;
    max_data_core_degree = 1;
    min_cross_access_read = 0;
    max_cross_access_read = 0;
    tot_cross_access_read = 0;
    min_cross_access_write = 0;
    max_cross_access_write = 0;
    tot_cross_access_write = 0;
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
    PRINT_INFO(lu, "Rnd-Num-Single-Core-Data",
               random_cluster.num_single_core_data);
    PRINT_INFO(lu, "Rnd-Min-Data-Core-Degree",
               random_cluster.min_data_core_degree);
    PRINT_INFO(lu, "Rnd-Max-Data-Core-Degree",
               random_cluster.max_data_core_degree);
    PRINT_INFO(lu, "Rnd-Min-Txn-Cross-Access-Read",
               random_cluster.min_cross_access_read);
    PRINT_INFO(lu, "Rnd-Max-Txn-Cross-Access-Read",
               random_cluster.max_cross_access_read);
    PRINT_INFO(lu, "Rnd-Total-Txn-Cross-Access-Read",
               random_cluster.tot_cross_access_read);
    PRINT_INFO(lu, "Rnd-Min-Txn-Cross-Access-Write",
               random_cluster.min_cross_access_write);
    PRINT_INFO(lu, "Rnd-Max-Txn-Cross-Access-Write",
               random_cluster.max_cross_access_write);
    PRINT_INFO(lu, "Rnd-Total-Txn-Cross-Access-Write",
               random_cluster.tot_cross_access_write);

    PRINT_INFO(lu, "Num-Single-Core-Data", output_cluster.num_single_core_data);
    PRINT_INFO(lu, "Min-Data-Core-Degree", output_cluster.min_data_core_degree);
    PRINT_INFO(lu, "Max-Data-Core-Degree", output_cluster.max_data_core_degree);
    PRINT_INFO(lu, "Min-Txn-Cross-Access-Read",
               output_cluster.min_cross_access_read);
    PRINT_INFO(lu, "Max-Txn-Cross-Access-Read",
               output_cluster.max_cross_access_read);
    PRINT_INFO(lu, "Total-Txn-Cross-Access-Read",
               output_cluster.tot_cross_access_read);
    PRINT_INFO(lu, "Min-Txn-Cross-Access-Write",
               output_cluster.min_cross_access_write);
    PRINT_INFO(lu, "Max-Txn-Cross-Access-Write",
               output_cluster.max_cross_access_write);
    PRINT_INFO(lu, "Total-Txn-Cross-Access-Write",
               output_cluster.tot_cross_access_write);
  }
};

#define ACCUMULATE_MIN(a, b) a = min(a, static_cast<uint64_t>(b))
#define ACCUMULATE_MAX(a, b) a = max(a, static_cast<uint64_t>(b))
#define ACCUMULATE_SUM(a, b) a += static_cast<uint64_t>(b)
#endif // DBX1000_PARTITIONED_HELPER_H
