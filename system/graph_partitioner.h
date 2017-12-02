#ifndef DBX1000_GRAPH_CREATOR_H
#define DBX1000_GRAPH_CREATOR_H

#include "global.h"
#include <vector>
#include "metis.h"

struct Graph {
    //Additional details


    //Inputs to the partitioner
    idx_t nvtxs;
    idx_t   ncon;
    idx_t adjncy_size;

    idx_t * xadj;
    idx_t * adjncy;
    idx_t * adjwgt;
    idx_t * vwgt;

    Graph() {
        nvtxs       = 0;
        ncon        = 0;
        adjncy_size = 0;
        xadj        = nullptr;
        adjncy      = nullptr;
        vwgt        = nullptr;
        adjwgt      = nullptr;
    }

    void release() {
        delete xadj;
        delete adjncy;
        delete vwgt;
        delete adjwgt;
    }
};

class CSRGraphCreator {
public:
    void    begin(uint32_t num_vertices);
    void    finish();
    void    move_to_next_vertex(idx_t vwgt = 1);
    void    add_edge(uint64_t adj_vertex, int weight);
    Graph * get_graph() { return graph; }
protected:
    idx_t   cvtx;
    idx_t   cpos;
    Graph * graph;
    std::vector<idx_t> adjncies;
    std::vector<idx_t> adjwgts;
};

class METISGraphPartitioner {
    idx_t       objval;
    idx_t       nparts;
    idx_t *     parts;
    idx_t *     options;
    Graph *     graph;
public:
    void        partition       (Graph * _graph, int num_partitions);
    uint32_t    get_partition   (uint64_t vtx);
};

inline uint32_t METISGraphPartitioner::get_partition(uint64_t vtx) {
    assert((idx_t) vtx < graph->nvtxs);
    return static_cast<uint32_t>(parts[vtx]);
}

#endif //DBX1000_GRAPH_CREATOR_H
