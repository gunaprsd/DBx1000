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

/*
 * Construct the conflict graph and use METIS graph partitioner
 * library to cluster the transaction nodes with the size constraints
 */
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

/*
 * Construct the access graph and use METIS
 * graph partitioning library to cluster both the
 * transaction and data nodes with size constraints
 */
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

/*
 * Construct the bipartite access graph and use a two-step
 * iterative algorithm to converge to a solution.
 * 1. Initially data is allocated randomly.
 * 2. Allot transaction based on which core yields maximum savings
 * 3. Re-allot data items based on which core has maximum number of
 * txns accessing it
 */
class HeuristicPartitioner1 : public BasePartitioner {
  public:
    HeuristicPartitioner1(uint32_t num_clusters);
  protected:
    virtual void internal_txn_partition();
    virtual void internal_data_partition();
    virtual void init_data_partition();
    virtual void do_partition() override;
};

/*
 * A variant of two step algorithm. Allot transaction also
 * based on the impact of savings.
 */
class HeuristicPartitioner2 : public HeuristicPartitioner1 {
  public:
    HeuristicPartitioner2(uint32_t num_clusters);
  protected:
    virtual void internal_txn_partition() override;
};

/*
 * Another variant in which initial data partition is done
 * more carefully.
 */
class HeuristicPartitioner3 : public HeuristicPartitioner1 {
  public:
    HeuristicPartitioner3(uint32_t num_clusters);
  protected:
    void init_data_partition();
};

/*
 * A k-means partitioner that represents each transaction in a d-dimensional
 * space and clusters them based on k-means clustering.
 */
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

/*
 * Finds the connected components through a breadth-first search
 * of the access graph.
 */
class ConnectedComponentPartitioner: public BasePartitioner {
public:
	explicit ConnectedComponentPartitioner(uint32_t num_clusters);
protected:
	void do_partition();
};

class UnionFindPartitioner : public BasePartitioner
{
public:
	explicit UnionFindPartitioner(uint32_t num_clusters);

protected:
	void do_partition();
private:
	int64_t Find(int64_t p);
	void Union(int64_t p, int64_t q);
};

#endif // DBX1000_PARTITIONER_H
