#ifndef DBX1000_PARTITIONED_HELPER_H
#define DBX1000_PARTITIONED_HELPER_H

#include "global.h"
#include <cstring>
#include <metis.h>
#include <system/query.h>

struct DataNodeInfo {
    idx_t id;
    uint32_t tid;
    uint64_t epoch;
    uint64_t iteration;
    idx_t assigned_core;
    bool single_core;
    uint64_t core_weights[MAX_NUM_CORES];
    std::vector<idx_t> read_txns;
    std::vector<idx_t> write_txns;

    DataNodeInfo() {}

    void reset(idx_t _id, uint64_t _epoch, uint32_t _table_id) {
        id = _id;
        tid = _table_id;
        epoch = _epoch;
        iteration = 0;
        assigned_core = UINT64_MAX;
        single_core = false;
        read_txns.clear();
        write_txns.clear();
        memset(&core_weights, 0, sizeof(uint64_t) * MAX_NUM_CORES);
    }
};

struct TxnNodeInfo {
    idx_t id;
    uint64_t epoch;
    uint64_t iteration;
    idx_t assigned_core;
    ReadWriteSet rwset;
    uint64_t savings[MAX_NUM_CORES];

    TxnNodeInfo() {}

    void reset(idx_t _id, uint64_t _epoch) {
        id = _id;
        epoch = _epoch;
        iteration = 0;
        assigned_core = UINT64_MAX;
        rwset.num_accesses = 0;
        memset(&savings, 0, sizeof(uint64_t) * MAX_NUM_CORES);
    }
};

struct TableInfo {
    uint32_t id;
    uint64_t num_accessed_data;
    uint64_t num_total_accesses;
    vector<uint64_t> txn_cross_access_histogram;
    vector<uint64_t> data_core_degree_histogram;

    TableInfo() {}
    void initialize(uint32_t _id, uint64_t max_core_degree, uint64_t max_txn_access) {
        id = _id;
        for (uint64_t i = 0; i < max_core_degree; i++) {
            data_core_degree_histogram.push_back(0);
        }
        for (uint64_t i = 0; i < max_txn_access; i++) {
            txn_cross_access_histogram.push_back(0);
        }
    }

    void reset() {
        num_accessed_data = 0;
        num_total_accesses = 0;
        for (uint64_t i = 0; i < data_core_degree_histogram.size(); i++) {
            data_core_degree_histogram[i] = 0;
        }

        for (uint64_t i = 0; i < txn_cross_access_histogram.size(); i++) {
            txn_cross_access_histogram[i] = 0;
        }
    }

    void print(string name) {
        printf("%s-%-25s: %lu\n", name.c_str(), "Total-Accesses", num_total_accesses);
        printf("%s-%-25s: %lu\n", name.c_str(), "Num-Accessed-Data", num_accessed_data);
        printf("%s-%-25s: ", name.c_str(), "Txn-Cross-Access-Histogram");
        uint64_t stop_index = txn_cross_access_histogram.size();
        for (; stop_index > 0u; stop_index--) {
            if (txn_cross_access_histogram[stop_index - 1] > 0) {
                break;
            }
        }
        for (uint64_t i = 0; i < stop_index; i++) {
            printf("%lu, ", txn_cross_access_histogram[i]);
        }
        printf("\n");

        printf("%s-%-25s: ", name.c_str(), "Data-Core-Degree-Histogram");
        stop_index = data_core_degree_histogram.size();
        for (; stop_index > 0u; stop_index--) {
            if (data_core_degree_histogram[stop_index - 1] > 0.01) {
                break;
            }
        }
        double total_weighted_num_cores = 0.0;
        for (uint64_t i = 0; i < stop_index; i++) {
            double fraction =
                (double)data_core_degree_histogram[i] * 100.0 / (double)num_accessed_data;
            printf("%5.2lf, ", fraction);
            total_weighted_num_cores += data_core_degree_histogram[i] * (i + 1);
        }
        printf("\n");

        if (num_accessed_data > 0) {
            printf("%s-%-25s: %5.2lf\n", name.c_str(), "Weighted-Avg-Cores",
                   total_weighted_num_cores / num_accessed_data);
        } else {
            printf("%s-%-25s: %5.2lf\n", name.c_str(), "Weighted-Avg-Cores", 0.0);
        }
    }
};

struct GraphInfo {
    uint64_t num_txn_nodes;
    uint64_t num_data_nodes;
    uint64_t num_edges;
    uint64_t min_data_degree;
    uint64_t max_data_degree;
    uint64_t min_txn_degree;
    uint64_t max_txn_degree;
    TxnNodeInfo *txn_info;
    DataNodeInfo *data_info;
    vector<uint64_t> data_inv_idx;
    GraphInfo(uint64_t max_data_nodes, uint64_t max_txn_nodes) :
            num_txn_nodes(0),
            num_data_nodes(0),
            num_edges(0),
            min_data_degree(UINT64_MAX),
            max_data_degree(0),
            min_txn_degree(UINT64_MAX),
            max_txn_degree(0),
            txn_info(nullptr),
            data_info(nullptr),
            data_inv_idx()
    {
        txn_info = new TxnNodeInfo[max_txn_nodes];
        data_info = new DataNodeInfo[max_data_nodes];
        reset();
    }

    void reset() {
        num_txn_nodes = 0;
        num_data_nodes = 0;
        num_edges = 0;
        min_data_degree = UINT64_MAX;
        max_data_degree = 0;
        min_txn_degree = UINT64_MAX;
        max_txn_degree = 0;
        data_inv_idx.clear();
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

struct ClusterInfo {
    uint32_t num_clusters;
    uint32_t num_tables;
    uint64_t objective;
    vector<TableInfo *> table_info;
    ClusterInfo(uint32_t num_clusters_, uint32_t num_tables_)
        : num_clusters(num_clusters_),
          num_tables(num_tables_),
          objective(0),
          table_info() {
        for (uint32_t i = 0; i < num_tables; i++) {
            table_info.push_back(new TableInfo());
            table_info[i]->initialize(i, num_clusters, MAX_NUM_ACCESSES);
        }
    }

    void initialize() {
        table_info.clear();
        for (uint32_t i = 0; i < num_tables; i++) {
            table_info.push_back(new TableInfo());
            table_info[i]->initialize(i, num_clusters, MAX_NUM_ACCESSES);
        }
    }

    void reset() {
        objective = 0;
        for (auto tinfo : table_info) {
            tinfo->reset();
        }
    }

    void print() {
        PRINT_INFO(lu, "Objective", objective);
        for (auto tinfo : table_info) {
            tinfo->print("Table" + std::to_string(tinfo->id));
        }
    }
};

struct RuntimeInfo {
    double rwset_duration;
    double preprocessing_duration;
    double partition_duration;

    RuntimeInfo() { reset(); }

    void reset() {
        preprocessing_duration = 0;
        partition_duration = 0;
        rwset_duration = 0;
    }

    void print() {
        PRINT_INFO(-10lf, "ReadWriteSet-Duration", rwset_duration);
        PRINT_INFO(-10lf, "PreProcessing-Duration", preprocessing_duration);
        PRINT_INFO(-10lf, "Partition-Duration", partition_duration);
    }
};

#define ACCUMULATE_MIN(a, b) a = min(a, static_cast<uint64_t>(b))
#define ACCUMULATE_MAX(a, b) a = max(a, static_cast<uint64_t>(b))
#endif // DBX1000_PARTITIONED_HELPER_H
