#include "partitioner.h"

BasePartitioner::BasePartitioner(uint32_t num_clusters) : _num_clusters(num_clusters), _rand(1), iteration(1) {
    _rand.seed(0, FLAGS_seed + 125);
}

void BasePartitioner::partition(GraphInfo *_graph_info, ClusterInfo *_cluster_info,
                                RuntimeInfo *_runtime_info) {
    graph_info = _graph_info;
    cluster_info = _cluster_info;
    runtime_info = _runtime_info;

	init_random_partition();
	printf("********** Random Clustering ************\n");
	compute_cluster_info();
	cluster_info->print();

    do_partition();
}

void BasePartitioner::compute_cluster_info() {
    cluster_info->reset();
    cluster_info->objective = 0;
    for (size_t i = 0; i < graph_info->data_inv_idx.size(); i++) {
        auto key = graph_info->data_inv_idx[i];
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

        // Update data information - core and if single-core-only
        info->single_core = (num_c == 1);
        info->assigned_core = chosen_c;

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

        // update table wise info
        auto table_info = cluster_info->table_info[info->tid];
        table_info->num_accessed_data++;
        table_info->num_total_accesses += sum_c;
        table_info->data_core_degree_histogram[num_c - 1]++;
    }
}

void BasePartitioner::init_random_partition() {
	idx_t * parts = new idx_t[graph_info->num_txn_nodes];
	for(uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
		parts[i] = _rand.nextInt64(0) % _num_clusters;
	}
	assign_txn_clusters(parts);
	delete[] parts;
}

void BasePartitioner::assign_txn_clusters(idx_t *parts) {
	iteration++;
	for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {
		auto chosen_core = parts[i];
		graph_info->txn_info[i].assigned_core = chosen_core;
		auto query = &(graph_info->txn_info[i].rwset);
		for (auto j = 0u; j < query->num_accesses; j++) {
			auto info = &(graph_info->data_info[query->accesses[j].key]);
			if (info->iteration != iteration) {
				for (auto k = 0u; k < _num_clusters; k++) {
					info->core_weights[k] = 0;
				}
				info->iteration = iteration;
			}
			info->core_weights[chosen_core]++;
		}
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
                    if (rw1->accesses[k].key == rw2->accesses[l].key && conflict) {
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
    runtime_info->preprocessing_duration = DURATION(end_time, start_time);

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
    runtime_info->partition_duration = DURATION(end_time, start_time);

	assign_txn_clusters(parts);

    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();

	delete[] parts;
}

AccessGraphPartitioner::AccessGraphPartitioner(uint32_t num_clusters)
    : BasePartitioner(num_clusters), vwgt(), adjwgt(), xadj(), adjncy(), vsize() {}

void AccessGraphPartitioner::add_txn_nodes() {
    ReadWriteSet *rwset;
    for (auto i = 0u; i < graph_info->num_txn_nodes; i++) {
        rwset = &(graph_info->txn_info[i].rwset);
        for (auto j = 0u; j < rwset->num_accesses; j++) {
            auto info = &(graph_info->data_info[rwset->accesses[j].key]);

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
    for (size_t i = 0; i < graph_info->data_inv_idx.size(); i++) {
        auto key = graph_info->data_inv_idx[i];
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
    vsize.reserve(total_num_vertices);
    adjncy.reserve(2 * graph_info->num_edges);
    adjwgt.reserve(2 * graph_info->num_edges);

    auto start_time = get_server_clock();
    xadj.push_back(0);
    add_txn_nodes();
    add_data_nodes();
    auto end_time = get_server_clock();
    runtime_info->preprocessing_duration = DURATION(end_time, start_time);

    auto graph = new METIS_CSRGraph();
    graph->nvtxs = total_num_vertices;
    graph->adjncy_size = 2 * graph_info->num_edges;
    graph->vwgt = vwgt.data();
    graph->xadj = xadj.data();
    graph->adjncy = adjncy.data();
    graph->adjwgt = adjwgt.data();
    graph->ncon = 1;

    auto all_parts = new idx_t[total_num_vertices];
    for (uint64_t i = 0; i < total_num_vertices; i++) {
        all_parts[i] = -1;
    }

    start_time = get_server_clock();
    METISGraphPartitioner::compute_partitions(graph, _num_clusters, all_parts);
    end_time = get_server_clock();
    runtime_info->partition_duration = DURATION(end_time, start_time);

    assign_txn_clusters(all_parts);

    delete[] all_parts;
    xadj.clear();
    vwgt.clear();
    adjncy.clear();
    adjwgt.clear();
}

ApproximateGraphPartitioner::ApproximateGraphPartitioner(uint32_t num_clusters)
    : BasePartitioner(num_clusters) {}

void ApproximateGraphPartitioner::internal_txn_partition(uint64_t iteration) {
    assert(_num_clusters < MAX_NUM_CORES);
    auto _cluster_size = new uint64_t[_num_clusters];
    memset(_cluster_size, 0, sizeof(uint64_t) * _num_clusters);

    double max_cluster_size =
        ((1000 + FLAGS_ufactor) * graph_info->num_edges) / (_num_clusters * 1000.0);
    // double max_cluster_size = UINT64_MAX;

    uint64_t *sorted = new uint64_t[_num_clusters];

    for (uint64_t i = 0; i < graph_info->num_txn_nodes; i++) {

        auto query = &(graph_info->txn_info[i].rwset);
        memset(graph_info->txn_info[i].savings, 0, sizeof(uint64_t) * MAX_NUM_CORES);

        for (auto j = 0u; j < query->num_accesses; j++) {
            auto info = &(graph_info->data_info[query->accesses[j].key]);
            auto core = info->assigned_core;
            if (query->accesses[j].access_type == RD) {
                graph_info->txn_info[i].savings[core] += 1 + info->write_txns.size();
            } else {
                graph_info->txn_info[i].savings[core] +=
                    1 + (info->read_txns.size() + info->write_txns.size());
            }
        }

        sort_helper(sorted, graph_info->txn_info[i].savings, _num_clusters);

        bool allotted = false;
        uint64_t chosen_core = UINT32_MAX;
        if (FLAGS_stdev_partitioner) {

            /*
             * Compute the mean and standard deviation of savings
             */
            double sum = 0, sum_sq = 0;
            uint64_t min_core = UINT32_MAX, min_cluster_size = UINT64_MAX;
            for (uint64_t s = 0; s < _num_clusters; s++) {
                double saving = (double)graph_info->txn_info[i].savings[s];
                sum += saving;
                sum_sq += saving * saving;
                if (_cluster_size[s] < min_cluster_size) {
                    min_cluster_size = _cluster_size[s];
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
                    if (_cluster_size[core] + query->num_accesses < max_cluster_size) {
                        chosen_core = core;
                        allotted = true;
                    }
                } else {
                    // Put it in the cluster with smallest size
                    chosen_core = min_core;
                    allotted = true;
                }
            }
        } else {

            /*
             * Put it in the core with the maximum savings and
             * that satisfies the size constraint.
             */
            for (uint64_t s = 0; s < _num_clusters && !allotted; s++) {
                auto core = sorted[s];
                if (_cluster_size[core] + query->num_accesses < max_cluster_size) {
                    chosen_core = core;
                    allotted = true;
                }
            }
        }

        assert(allotted);
        graph_info->txn_info[i].assigned_core = chosen_core;
        _cluster_size[chosen_core] += query->num_accesses;

        for (auto j = 0u; j < query->num_accesses; j++) {
            auto info = &(graph_info->data_info[query->accesses[j].key]);
            if (info->iteration != iteration) {
                for (auto k = 0u; k < _num_clusters; k++) {
                    info->core_weights[k] = 0;
                }
                info->iteration = iteration;
            }
            info->core_weights[chosen_core]++;
        }
    }

    delete[] sorted;
}

void ApproximateGraphPartitioner::internal_data_partition(uint64_t iteration) {
    uint64_t *core_weights = new uint64_t[_num_clusters];
    for (size_t i = 0; i < graph_info->data_inv_idx.size(); i++) {
        auto key = graph_info->data_inv_idx[i];
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
}

void ApproximateGraphPartitioner::init_data_partition() {
    // TODO do something!!
}

void ApproximateGraphPartitioner::sort_helper(uint64_t *index, uint64_t *value, uint64_t size) {
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

void ApproximateGraphPartitioner::do_partition()  {
    init_data_partition();
    uint64_t start_time, end_time;
    for (uint32_t i = 0; i < FLAGS_iterations; i++) {
        start_time = get_server_clock();
        // partition data based on transaction allocation
        internal_data_partition(i);
        // partition txn based on data allocation.
        internal_txn_partition(i);
        end_time = get_server_clock();
        runtime_info->partition_duration += DURATION(end_time, start_time);
    }
}