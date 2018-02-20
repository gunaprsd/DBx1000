#ifndef DBX1000_PARTITIONER_H
#define DBX1000_PARTITIONER_H
#include "distributions.h"
#include "global.h"
#include "graph_partitioner.h"
#include "partitioner_helper.h"
#include "query.h"
#include <algorithm>
#include <cstring>

class BasePartitioner {
  public:
    void partition(GraphInfo *_graph_info, ClusterInfo *_cluster_info, RuntimeInfo *_runtime_info);
  protected:
    uint32_t _num_clusters;
    GraphInfo *graph_info;
    ClusterInfo *cluster_info;
    RuntimeInfo *runtime_info;
    RandomNumberGenerator _rand;
	uint64_t iteration;
    BasePartitioner(uint32_t num_clusters);
	void init_random_partition();
	void assign_txn_clusters(idx_t* parts);
    void compute_cluster_info();
    virtual void do_partition() = 0;
};

class ConflictGraphPartitioner : public BasePartitioner {
  public:
    ConflictGraphPartitioner(uint32_t num_clusters);

  protected:
    vector<idx_t> vwgt;
    vector<idx_t> adjwgt;
    vector<idx_t> xadj;
    vector<idx_t> adjncy;
    void create_graph();
    void do_partition() override;
};

class AccessGraphPartitioner : public BasePartitioner {
  public:
    AccessGraphPartitioner(uint32_t num_clusters);

  protected:
    vector<idx_t> vwgt;
    vector<idx_t> adjwgt;
    vector<idx_t> xadj;
    vector<idx_t> adjncy;
    vector<idx_t> vsize;
    void add_txn_nodes();
    void add_data_nodes();
    void do_partition() override;
};

class ApproximateGraphPartitioner : public BasePartitioner {
  public:
    ApproximateGraphPartitioner(uint32_t num_clusters);
  protected:
    void internal_txn_partition(uint64_t iteration);
    void internal_data_partition(uint64_t iteration);
    void init_data_partition();
    void sort_helper(uint64_t *index, uint64_t *value, uint64_t size);
    void do_partition() override;
};

#endif // DBX1000_PARTITIONER_H
