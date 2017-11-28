#ifndef DBX1000_GRAPH_CREATOR_H
#define DBX1000_GRAPH_CREATOR_H

#include "global.h"
#include <vector>
#include "metis.h"

struct Graph {
    //Additional details
    idx_t nedges;
    idx_t adjncy_size;

    //Inputs to the partitioner
    idx_t nvtxs;
    idx_t ncon;
    idx_t * xadj;
    idx_t * adjncy;
    idx_t * vwgt;
    idx_t * adjwgt;
    idx_t nparts;
    idx_t * options;

    //Outputs from the partitioner
    idx_t objval;
    idx_t * parts;

    Graph() {
        reset();
    }

    void reset() {
        nedges      = 0;
        adjncy_size = 0;

        nvtxs   = 0;
        ncon    = 0;
        xadj    = nullptr;
        adjncy  = nullptr;
        vwgt    = nullptr;
        adjwgt  = nullptr;
        nparts  = 0;
        options = nullptr;

        objval  = 0;
        parts   = nullptr;
    }

    void release() {
        delete xadj;
        delete adjncy;
        delete vwgt;
        delete adjwgt;
        delete options;
        delete parts;
    }
};

class GraphCreator {
public:
    void initialize(uint32_t num_vertices);

    void move_to_next_vertex();

    void add_edge(uint32_t adj_vertex, uint32_t weight);

    void finish();

    void do_cluster(int num_clusters);

    uint32_t get_cluster_id(uint32_t vtx) {
        assert(vtx < graph->nvtxs);
        assert(graph->parts != NULL);
        return graph->parts[vtx];
    }

    void release() {
        graph->release();
        graph->reset();
    }

protected:
    void prepare();

    Graph * graph;

    uint32_t init_batch_size;

    std::vector<idx_t *> adjncies;
    std::vector<idx_t *> adjwgts;

    idx_t cvtx;
    idx_t cpos;
    idx_t current_index;
    idx_t current_offset;
    idx_t current_batch_size;

    idx_t * current_adjncy ;
    idx_t * current_adjwgt ;
};

#endif //DBX1000_GRAPH_CREATOR_H
