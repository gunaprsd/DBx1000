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
    void partition(uint64_t id, GraphInfo *_graph_info, ClusterInfo *_cluster_info,
                   RuntimeInfo *_runtime_info);

  protected:
    uint64_t old_objective_value;
    bool converged;
    uint32_t _num_clusters;
    GraphInfo *graph_info;
    ClusterInfo *cluster_info;
    RuntimeInfo *runtime_info;
    RandomNumberGenerator _rand;
    uint64_t iteration;
    uint64_t id;
    BasePartitioner(uint32_t num_clusters);
    void sort_helper(uint64_t *index, uint64_t *value, uint64_t size);
    void init_random_partition();
    void assign_txn_clusters(idx_t *parts);
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
    void add_txn_nodes();
    void add_data_nodes();
    void do_partition() override;
};

class HeuristicPartitioner1 : public BasePartitioner {
  public:
    HeuristicPartitioner1(uint32_t num_clusters);

  protected:
    virtual void internal_txn_partition();
    virtual void internal_data_partition();
    virtual void init_data_partition();
    virtual void do_partition() override;
};

class HeuristicPartitioner2 : public HeuristicPartitioner1 {
  public:
    HeuristicPartitioner2(uint32_t num_clusters);

  protected:
    virtual void internal_txn_partition(uint64_t iteration);
};

class HeuristicPartitioner3 : public HeuristicPartitioner2 {
  public:
    HeuristicPartitioner3(uint32_t num_clusters);

  protected:
    void init_data_partition();
};

class KMeansPartitioner : public BasePartitioner {
public:
	KMeansPartitioner(uint32_t num_clusters);
protected:
	void do_partition();
	void do_iteration();
	const uint32_t dim;
	double* txn;
	double* means;
};

class ConnectedComponentPartitioner: public BasePartitioner {
public:
	explicit ConnectedComponentPartitioner(uint32_t num_clusters);
protected:
	void do_partition();
};

#endif // DBX1000_PARTITIONER_H
