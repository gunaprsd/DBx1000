#include <queue>
#include <stack>
#include "partitioner.h"
using namespace std;

BasePartitioner::BasePartitioner(uint32_t num_clusters)
    : old_objective_value(0), converged(false), _num_clusters(num_clusters), _rand(1),
      iteration(1) {
    _rand.seed(0, FLAGS_seed + 125);
}

void BasePartitioner::partition(uint64_t _id, GraphInfo *_graph_info, ClusterInfo *_cluster_info,
                                RuntimeInfo *_runtime_info) {
    id = _id;
    graph_info = _graph_info;
    cluster_info = _cluster_info;
    runtime_info = _runtime_info;
    iteration = 1;

    init_random_partition();
    compute_cluster_info();
    printf("********** (Batch %lu) Random Cluster Information ************\n", id);
    cluster_info->print();

    // do_partition must assign core_weights to data node
    // and assigned_core for each txn node.
    iteration++;
    do_partition();
    compute_cluster_info();
}

/*
 * Assumes that core_weights for data items are set
 * according to transaction to core allocation.
 */
void BasePartitioner::compute_cluster_info() {
    old_objective_value = cluster_info->objective;
    cluster_info->reset();
    cluster_info->objective = 0;
    for (auto key : graph_info->data_inv_idx) {
        auto info = &(graph_info->data_info[key]);
        uint64_t sum_c_sq = 0, sum_c = 0, num_c = 0;
        uint64_t max_c = 0, chosen_c = UINT64_MAX;
        for (uint64_t c = 0; c < _num_clusters; c++) {
            auto val = info->core_weights[c];
            sum_c_sq += (val * val);
            sum_c += val;
            num_c += val > 0 ? 1 : 0;
            if (val > max_c) {
                max_c = val;
                chosen_c = c;
            }
        }

        /*
         * Update objective info
         * ---------------------
         * The objective for clustering is to minimize sum of weights of
         * straddler edges in conflict graph. To compute it, we
         * can do it per-data item as follows:
         * \sum_{d \in D} \sum_{T_i, T_j not in same core and access d} 1
         * For each data item,
         * \sum_{T_i, T_j not in same core} 1 = (All pairs count - \sum_{T_i, T_j
         * in same core})/2
         */
        cluster_info->objective += ((sum_c * sum_c) - sum_c_sq) / 2;

        // Update data information - core and if single-core-only
        info->single_core = (num_c == 1);
        info->assigned_core = chosen_c;

        // update table wise info
        auto table_info = &cluster_info->table_info[info->tid];
        table_info->num_accessed_data++;
        table_info->num_total_accesses += sum_c;
        table_info->data_core_degree_histogram[num_c - 1]++;
    }

    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        auto txn_info = &(graph_info->txn_info[i]);
        cluster_info->cluster_size[txn_info->assigned_core]++;
    }

    if (old_objective_value == cluster_info->objective) {
        converged = true;
    }
}

void BasePartitioner::init_random_partition() {
    idx_t *parts = new idx_t[graph_info->num_txn_nodes];
    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        parts[i] = _rand.nextInt64(0) % _num_clusters;
    }
    assign_txn_clusters(parts);
    delete[] parts;
}

void BasePartitioner::assign_txn_clusters(idx_t *parts) {
    // increase iteration so that we are able to update
    // core_weights appropriately and not confuse with old
    // values that it may contain.
    iteration++;

    // memset(cluster_info->cluster_size, 0, sizeof(uint64_t) * _num_clusters);

    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        auto chosen_core = parts[i];
        // assign the core to txn
        graph_info->txn_info[i].assigned_core = chosen_core;

        // update core_weights of data nodes
        auto rwset = &(graph_info->txn_info[i].rwset);
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            if (info->iteration != iteration) {
                // first time we are seeing this data item
                // reset the core_weights
                memset(info->core_weights, 0, sizeof(uint64_t) * MAX_NUM_CORES);
                info->iteration = iteration;
            }
            info->core_weights[chosen_core]++;
        }

        // update cluster size info
        // cluster_info->cluster_size[chosen_core] += graph_info->txn_info[i].rwset.num_accesses;
    }
}

void BasePartitioner::sort_helper(uint64_t *index, uint64_t *value, uint64_t size) {
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

ConflictGraphPartitioner::ConflictGraphPartitioner(uint32_t num_clusters)
    : BasePartitioner(num_clusters), vwgt(), adjwgt(), xadj(), adjncy() {}

void ConflictGraphPartitioner::create_graph() {
    ReadWriteSet *rw1, *rw2;
    xadj.push_back(0);
    for (auto i = 0u; i < graph_info->num_txn_nodes; i++) {
        rw1 = &(graph_info->txn_info[i].rwset);

        for (auto j = 0u; j < graph_info->num_txn_nodes; j++) {
            rw2 = &(graph_info->txn_info[j].rwset);

            if (i == j) {
                break;
            }

            // Compute edge weight for T_i, T_j
            uint64_t edge_weight = 0;
            for (auto k = 0u; k < rw1->num_accesses; k++) {
                for (auto l = 0u; l < rw2->num_accesses; l++) {
                    bool conflict = !((rw1->accesses[k].access_type == RD) &&
                                      (rw2->accesses[l].access_type == RD));
                    if (conflict && (rw1->accesses[k].key == rw2->accesses[l].key)) {
                        edge_weight++;
                    }
                }
            }

            if (edge_weight > 0) {
                adjncy.push_back(j);
                adjwgt.push_back(edge_weight);
            }
        }

        xadj.push_back(static_cast<idx_t>(adjncy.size()));
        vwgt.push_back(rw1->num_accesses);
    }
}

void ConflictGraphPartitioner::do_partition() {
    xadj.reserve(graph_info->num_txn_nodes + 1);
    vwgt.reserve(graph_info->num_txn_nodes);
    adjncy.reserve(2 * graph_info->num_edges);
    adjwgt.reserve(2 * graph_info->num_edges);

    auto start_time = get_server_clock();
    create_graph();
    auto end_time = get_server_clock();
    runtime_info->preprocessing_duration += DURATION(end_time, start_time);

    idx_t *parts = new idx_t[graph_info->num_txn_nodes];
    auto graph = new METIS_CSRGraph();
    graph->nvtxs = graph_info->num_txn_nodes;
    graph->adjncy_size = static_cast<idx_t>(adjncy.size());
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->ncon = 1;

    start_time = get_server_clock();
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, parts);
    end_time = get_server_clock();
    runtime_info->partition_duration += DURATION(end_time, start_time);

    // assign the cores to txn based on this parts array
    assign_txn_clusters(parts);

    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();

    delete[] parts;
}

AccessGraphPartitioner::AccessGraphPartitioner(uint32_t num_clusters)
    : BasePartitioner(num_clusters), vwgt(), adjwgt(), xadj(), adjncy() {}

void AccessGraphPartitioner::add_txn_nodes() {
    ReadWriteSet *rwset;
    for (auto i = 0u; i < graph_info->num_txn_nodes; i++) {
        rwset = &(graph_info->txn_info[i].rwset);

        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);

            idx_t wt = 1;
            if (!FLAGS_unit_weights) {
                if (rwset->accesses[j].access_type == RD) {
                    wt += static_cast<idx_t>(info->write_txns.size());
                } else if (rwset->accesses[j].access_type == WR) {
                    wt += static_cast<idx_t>(info->read_txns.size() + info->write_txns.size());
                }
            }
            adjncy.push_back(info->id);
            adjwgt.push_back(wt);
        }

        xadj.push_back(static_cast<idx_t>(adjncy.size()));
        vwgt.push_back(rwset->num_accesses);
    }

    assert((size_t)graph_info->num_edges == adjncy.size());
    assert((size_t)graph_info->num_edges == adjwgt.size());
    assert((size_t)graph_info->num_txn_nodes == vwgt.size());
    assert((size_t)graph_info->num_txn_nodes == xadj.size() - 1);
}

void AccessGraphPartitioner::add_data_nodes() {
    for (auto key : graph_info->data_inv_idx) {
        auto info = &(graph_info->data_info[key]);

        // insert txn edges
        adjncy.insert(adjncy.end(), info->read_txns.begin(), info->read_txns.end());
        adjncy.insert(adjncy.end(), info->write_txns.begin(), info->write_txns.end());

        // insert edge weights
        idx_t read_wgt = 1, write_wgt = 1;
        if (!FLAGS_unit_weights) {
            read_wgt += static_cast<idx_t>(info->write_txns.size());
            write_wgt += static_cast<idx_t>(info->read_txns.size() + info->write_txns.size());
        }

        adjwgt.insert(adjwgt.end(), info->read_txns.size(), read_wgt);
        adjwgt.insert(adjwgt.end(), info->write_txns.size(), write_wgt);

        // insert node details
        xadj.push_back(static_cast<idx_t>(adjncy.size()));
        vwgt.push_back(0);
    }

    assert(2 * (size_t)graph_info->num_edges == adjncy.size());
    assert(2 * (size_t)graph_info->num_edges == adjwgt.size());
    auto num_nodes = static_cast<size_t>(graph_info->num_data_nodes + graph_info->num_txn_nodes);
    assert(num_nodes == vwgt.size());
    assert(num_nodes + 1 == xadj.size());
}

void AccessGraphPartitioner::do_partition() {
    auto total_num_vertices = graph_info->num_txn_nodes + graph_info->num_data_nodes;
    xadj.reserve(total_num_vertices + 1);
    vwgt.reserve(total_num_vertices);
    adjncy.reserve(2 * graph_info->num_edges);
    adjwgt.reserve(2 * graph_info->num_edges);

    auto start_time = get_server_clock();
    xadj.push_back(0);
    add_txn_nodes();
    add_data_nodes();
    auto end_time = get_server_clock();
    runtime_info->preprocessing_duration += DURATION(end_time, start_time);

    auto graph = new METIS_CSRGraph();
    graph->nvtxs = total_num_vertices;
    graph->adjncy_size = 2 * graph_info->num_edges;
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->ncon = 1;

    auto all_parts = new idx_t[total_num_vertices];
    start_time = get_server_clock();
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, all_parts);
    end_time = get_server_clock();
    runtime_info->partition_duration += DURATION(end_time, start_time);

    assign_txn_clusters(all_parts);

    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();
    delete[] all_parts;
}

HeuristicPartitioner1::HeuristicPartitioner1(uint32_t num_clusters)
    : BasePartitioner(num_clusters) {}

/*
 * Reassign transactions to cores based on assigned core of data
 * items. Also, update core_weights for data items appropriately.
 */
void HeuristicPartitioner1::internal_txn_partition() {
    double compute_savings_duration = 0;
    double data_reallocation_duration = 0;

    assert(!converged);
    // change this to ensure that core_weights array for
    // data items are updated properly
    iteration++;

    // clear cluster size info
    memset(cluster_info->cluster_size, 0, sizeof(uint64_t) * _num_clusters);

    double max_cluster_size =
        ((1000 + FLAGS_ufactor) * graph_info->num_edges) / (_num_clusters * 1000.0);
    // double max_cluster_size = UINT64_MAX;

    uint64_t *sorted = new uint64_t[_num_clusters];
    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        auto rwset = &(graph_info->txn_info[i].rwset);

        // clear savings array of txn
        memset(graph_info->txn_info[i].savings, 0, sizeof(uint64_t) * MAX_NUM_CORES);

        uint64_t savings_compute_stime = get_server_clock();
        // compute savings array
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            auto core = info->assigned_core;
            if (rwset->accesses[j].access_type == RD) {
                graph_info->txn_info[i].savings[core] += (1 + info->write_txns.size());
            } else {
                graph_info->txn_info[i].savings[core] +=
                    (1 + (info->read_txns.size() + info->write_txns.size()));
            }
        }
        uint64_t savings_compute_etime = get_server_clock();
        compute_savings_duration += DURATION(savings_compute_etime, savings_compute_stime);

        for (auto j = 0u; j < _num_clusters; j++) {
            sorted[j] = j;
        }
        sort_helper(sorted, graph_info->txn_info[i].savings, _num_clusters);

        /*
         * Find earliest core with maximum savings and
         * that satisfies the size constraint.
         */
        bool allotted = false;
        uint64_t chosen_core = UINT32_MAX;
        for (uint64_t s = 0; s < _num_clusters && !allotted; s++) {
            auto core = sorted[s];
            if (cluster_info->cluster_size[core] + rwset->num_accesses < max_cluster_size) {
                chosen_core = core;
                allotted = true;
            }
        }
        assert(allotted);

        // assign core
        graph_info->txn_info[i].assigned_core = chosen_core;

        // update cluster size
        cluster_info->cluster_size[chosen_core] += rwset->num_accesses;

        uint64_t data_reallocation_stime = get_server_clock();
        // update core_weights array of all data items touched by txn
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            if (info->iteration != iteration) {
                memset(info->core_weights, 0, sizeof(uint64_t) * _num_clusters);
                info->iteration = iteration;
            }
            info->core_weights[chosen_core]++;
        }
        uint64_t data_reallocation_etime = get_server_clock();
        data_reallocation_duration += DURATION(data_reallocation_etime, data_reallocation_stime);
    }

    delete[] sorted;
    PRINT_INFO(lf, "Compute-Savings-Duration", compute_savings_duration);
    PRINT_INFO(lf, "Data-Reallocation-Duration", data_reallocation_duration);
}

void HeuristicPartitioner1::internal_data_partition() {
    auto start_time = get_server_clock();
    uint64_t *core_weights = new uint64_t[_num_clusters];
    for (auto key : graph_info->data_inv_idx) {
        auto info = &(graph_info->data_info[key]);
        uint64_t max_c = 0;
        uint64_t chosen_c = 0;
        for (uint64_t c = 0; c < _num_clusters; c++) {
            if (core_weights[c] > max_c) {
                max_c = core_weights[c];
                chosen_c = c;
            }
        }
        info->assigned_core = chosen_c;
    }
    auto end_time = get_server_clock();
    auto duration = DURATION(end_time, start_time);
    PRINT_INFO(lf, "Data-Core-Computation", duration);
}

void HeuristicPartitioner1::init_data_partition() {
    // Random allocation of data items
    for (auto key : graph_info->data_inv_idx) {
        auto info = &(graph_info->data_info[key]);
        info->assigned_core = -1;
    }

    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        auto rwset = &(graph_info->txn_info[i].rwset);

        idx_t chosen_core = i % _num_clusters;
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            if (info->assigned_core == -1) {
                info->assigned_core = chosen_core;
            }
        }
    }
}

void HeuristicPartitioner1::do_partition() {
    // random allocation
    init_data_partition();

    uint64_t start_time, end_time;
    for (uint32_t i = 0; i < FLAGS_iterations && !converged; i++) {
        start_time = get_server_clock();
        // cluster txn based on data allocation.
        internal_txn_partition();
        // cluster data based on transaction allocation
        internal_data_partition();
        end_time = get_server_clock();
        runtime_info->partition_duration += DURATION(end_time, start_time);

        // compute the cluster info
        compute_cluster_info();
        if (!converged) {
            printf("********** (Batch %lu) Cluster Information at Iteration %lu ************\n", id,
                   iteration - 2);
            cluster_info->print();
        }
    }
}

HeuristicPartitioner2::HeuristicPartitioner2(uint32_t num_clusters)
    : HeuristicPartitioner1(num_clusters) {}

void HeuristicPartitioner2::internal_txn_partition(uint64_t iteration) {
    // change this to ensure that core_weights array for
    // data items are updated properly
    iteration++;

    // clear cluster size info
    memset(cluster_info->cluster_size, 0, sizeof(uint64_t) * _num_clusters);

    double max_cluster_size =
        ((1000 + FLAGS_ufactor) * graph_info->num_edges) / (_num_clusters * 1000.0);
    // double max_cluster_size = UINT64_MAX;

    uint64_t *sorted = new uint64_t[_num_clusters];
    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        auto rwset = &(graph_info->txn_info[i].rwset);

        // clear savings array of txn
        memset(graph_info->txn_info[i].savings, 0, sizeof(uint64_t) * MAX_NUM_CORES);

        // compute savings array
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            auto core = info->assigned_core;
            if (rwset->accesses[j].access_type == RD) {
                graph_info->txn_info[i].savings[core] += 1 + info->write_txns.size();
            } else {
                graph_info->txn_info[i].savings[core] +=
                    1 + (info->read_txns.size() + info->write_txns.size());
            }
        }

        sort_helper(sorted, graph_info->txn_info[i].savings, _num_clusters);

        /*
         * Find earliest core with maximum savings and
         * that satisfies the size constraint.
         */
        bool allotted = false;
        uint64_t chosen_core = UINT32_MAX;
        double sum = 0, sum_sq = 0;
        uint64_t min_core = UINT32_MAX, min_cluster_size = UINT64_MAX;
        for (uint64_t s = 0; s < _num_clusters; s++) {
            double saving = (double)graph_info->txn_info[i].savings[s];
            sum += saving;
            sum_sq += saving * saving;
            if (cluster_info->cluster_size[s] < min_cluster_size) {
                min_cluster_size = cluster_info->cluster_size[s];
                min_core = s;
            }
        }
        double mean = sum / _num_clusters;
        double sq_mean = sum_sq / _num_clusters;
        double std_dev = sqrt(sq_mean - mean * mean);

        /*
         * Greedily allot the core with maximum savings if the chosen
         * core is *significantly* better.
         */
        for (uint64_t s = 0; s < _num_clusters && !allotted; s++) {
            auto core = sorted[s];
            double diff_ratio;
            if (std_dev > 0) {
                diff_ratio = (graph_info->txn_info[i].savings[core] - mean) / (1 + std_dev);
            } else {
                diff_ratio = 0;
            }

            if (diff_ratio > 1.0) {
                // Putting it in core makes some difference
                if (cluster_info->cluster_size[core] + rwset->num_accesses < max_cluster_size) {
                    chosen_core = core;
                    allotted = true;
                }
            } else {
                // Put it in the cluster with smallest size
                chosen_core = min_core;
                allotted = true;
            }
        }
        assert(allotted);

        // assign core
        graph_info->txn_info[i].assigned_core = chosen_core;

        // update cluster size
        cluster_info->cluster_size[chosen_core] += rwset->num_accesses;

        // update core_weights array of all data items touched by txn
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            if (info->iteration != iteration) {
                memset(info->core_weights, 0, sizeof(uint64_t) * _num_clusters);
                info->iteration = iteration;
            }
            info->core_weights[chosen_core]++;
        }
    }

    delete[] sorted;
}

HeuristicPartitioner3::HeuristicPartitioner3(uint32_t num_clusters)
    : HeuristicPartitioner2(num_clusters) {}

void HeuristicPartitioner3::init_data_partition() {
    // TODO do something!!
}

KMeansPartitioner::KMeansPartitioner(uint32_t num_clusters)
        : BasePartitioner(num_clusters), dim(FLAGS_kmeans_dim) {
}

void KMeansPartitioner::do_partition() {
    txn = new double[dim * graph_info->num_txn_nodes];
    means = new double[dim * _num_clusters];

    //initialize with txn vectors
    for(uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        for(uint64_t j = 0; j < dim; j++) {
            txn[i * dim + j] = 0;
        }
    }
    for(uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        auto rwset = &(graph_info->txn_info[i].rwset);
        for(uint64_t j = 0; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto part = key % dim;
            txn[i * dim + part] += 1;
        }
    }

    // initialize means
    for(uint64_t i = 0; i < _num_clusters; i++) {
        uint64_t chosen = _rand.nextInt64(0) % graph_info->num_txn_nodes;
        for(uint64_t j = 0; j < dim; j++) {
            means[i * dim + j] = txn[chosen * dim + j];
        }
    }

    for(uint32_t i = 0; i < FLAGS_iterations && !converged; i++) {
        auto start_time = get_server_clock();
        do_iteration();
        auto end_time = get_server_clock();
        runtime_info->partition_duration += DURATION(end_time, start_time);

        // compute the cluster info
        compute_cluster_info();
        if (!converged) {
            printf("********** (Batch %lu) Cluster Information at Iteration %lu ************\n", id,
                   iteration - 2);
            cluster_info->print();
        }
    }
    delete txn;
}

void KMeansPartitioner::do_iteration() {
    double duration = 0;
    double start_time, end_time;
    assert(!converged);
    // change this to ensure that core_weights array for
    // data items are updated properly
    iteration++;

    // data structure to store updated mean
    auto counts = new uint64_t[_num_clusters];
    auto other_means = new double[_num_clusters * dim];
    for(uint64_t k = 0; k < _num_clusters; k++) {
        counts[k] = 0;
        other_means[k] = 0;
    }

    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
        uint32_t chosen_core = 0;
        double min_distance = UINT64_MAX;

        start_time = get_server_clock();
        for(uint32_t j = 0; j < _num_clusters; j++) {
            double diff, distance = 0;
            for(uint64_t k = 0; k < dim; k++) {
                diff = (txn[i * dim + k] - means[j * dim + k]);
                distance += (diff * diff);
            }

            if(distance < min_distance) {
                min_distance = distance;
                chosen_core = j;
            }
        }

        // assign core
        graph_info->txn_info[i].assigned_core = chosen_core;

        // update cluster size
        auto rwset = &(graph_info->txn_info[i].rwset);
        cluster_info->cluster_size[chosen_core] += rwset->num_accesses;

        counts[chosen_core]++;
        for(uint64_t k = 0; k < dim; k++) {
            other_means[chosen_core * dim + k] += txn[i * dim + k];
        }

        end_time = get_server_clock();
        duration += DURATION(end_time, start_time);

        // update core_weights array of all data items touched by txn
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto key = rwset->accesses[j].key;
            auto info = &(graph_info->data_info[key]);
            if (info->iteration != iteration) {
                memset(info->core_weights, 0, sizeof(uint64_t) * _num_clusters);
                info->iteration = iteration;
            }
            info->core_weights[chosen_core]++;
        }
    }

    for(uint64_t i = 0; i < _num_clusters; i++) {
        if(counts[i] > 0) {
            for(uint64_t k = 0; k < dim; k++) {
                other_means[i * dim + k] /= counts[i];
            }
        }
    }
    memcpy(means, other_means, sizeof(double) * _num_clusters * dim);

    delete counts;
    delete other_means;

    PRINT_INFO(lf, "Iteration-Duration", duration);
}

ConnectedComponentPartitioner::ConnectedComponentPartitioner(uint32_t num_clusters) : BasePartitioner(num_clusters) {

}

void ConnectedComponentPartitioner::do_partition() {
    idx_t visited_count = 0;
    idx_t num_txn_nodes =  graph_info->num_txn_nodes;
    idx_t num_nodes = graph_info->num_txn_nodes + graph_info->num_data_nodes;
    idx_t unvisited_next_txn_node = 0;
    idx_t chosen_core = 0;
    while(visited_count < num_nodes) {
        // Take the next unvisited txn node and add to bfs queue
        queue<idx_t> nodes_queue;
        nodes_queue.push(unvisited_next_txn_node);

        while(!nodes_queue.empty()) {
            // Take the next node in the bfs queue
            idx_t node = nodes_queue.front();
            nodes_queue.pop();

            if(node > num_txn_nodes) {
                // obtain data node info
                idx_t inv_idx = node - graph_info->num_txn_nodes;
                uint64_t key = graph_info->data_inv_idx[inv_idx];
                auto info = &(graph_info->data_info[key]);

                // mark visited and assign core
                assert(info->iteration != iteration);
                info->assigned_core = chosen_core;
                info->iteration = iteration;

                // add all unvisited children to queue
                for(auto txn_id : info->read_txns) {
                    if(graph_info->txn_info[txn_id].iteration != iteration) {
                        nodes_queue.push(txn_id);
                    }
                }
                for(auto txn_id : info->write_txns) {
                    if(graph_info->txn_info[txn_id].iteration != iteration) {
                        nodes_queue.push(txn_id);
                    }
                }
            } else {
                // obtain txn node info
                auto info = &(graph_info->txn_info[node]);

                // mark visited and assign core
                assert(info->iteration != iteration);
                info->assigned_core = chosen_core;
                info->iteration = iteration;

                // add all unvisited children
                for(uint32_t i = 0; i < info->rwset.num_accesses; i++) {
                    auto key = info->rwset.accesses[i].key;
                    auto data_info = &(graph_info->data_info[key]);
                    if(data_info->iteration != iteration) {
                        for(uint64_t j = 0; j < _num_clusters; j++) {
                            data_info->core_weights[j] = 0;
                        }
                        nodes_queue.push(data_info->id);
                    }
                    data_info->core_weights[chosen_core]++;
                }
            }

            visited_count++;
        }

        if(visited_count < num_nodes) {
            for(; unvisited_next_txn_node < num_txn_nodes; unvisited_next_txn_node++) {
                if(graph_info->txn_info[unvisited_next_txn_node].iteration != iteration) {
                    break;
                }
            }
            chosen_core++;
        }
    }
}
