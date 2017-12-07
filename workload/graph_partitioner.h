#include "global.h"
#include "metis.h"
#include "helper.h"
#include <stdlib.h>
#include <assert.h>


#ifndef DBX1000_GRAPH_CREATOR_H
#define DBX1000_GRAPH_CREATOR_H

class METISGraphPartitioner;
class ParMETISGraphPartitioner;

struct CSRGraph {
    //Inputs to the partitioner
    idx_t nvtxs;
    idx_t   ncon;
    idx_t adjncy_size;

    idx_t * xadj;
    idx_t * adjncy;
    idx_t * adjwgt;
    idx_t * vwgt;

    CSRGraph() {
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

    void begin(uint32_t num_vertices)
    {
        graph           = new CSRGraph();
        graph->ncon     = 1;
        graph->nvtxs    = (idx_t) num_vertices;
        graph->xadj     = (idx_t *) malloc(sizeof(idx_t) * (graph->nvtxs + 1));
        graph->vwgt     = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);

        cvtx = 0;
        cpos = 0;
    }

    void finish()
    {
        assert(cvtx == graph->nvtxs);
        graph->xadj[cvtx] = cpos;

        graph->adjncy_size = cpos;
        graph->adjncy   = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);
        graph->adjwgt   = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);

        int i = 0;
        for(auto iter = adjncies.begin(); iter != adjncies.end(); iter++, i++) { graph->adjncy[i] = *iter; }
        i = 0;
        for(auto iter = adjwgts.begin(); iter != adjwgts.end(); iter++, i++) { graph->adjwgt[i] = *iter; }

        adjncies.clear();
        adjwgts.clear();
    }

    void move_to_next_vertex(idx_t vwgt = 1)
    {
        graph->xadj[cvtx] = cpos;
        graph->vwgt[cvtx] = 1;
        cvtx++;
    }

    void add_edge(uint64_t adj_vertex, int weight)
    {
        adjncies.push_back((idx_t)adj_vertex);
        adjwgts.push_back((idx_t)weight);
        cpos++;
    }

    CSRGraph * get_graph()
    {
        return graph;
    }
protected:
    idx_t   cvtx;
    idx_t   cpos;
    CSRGraph * graph;
    std::vector<idx_t> adjncies;
    std::vector<idx_t> adjwgts;
};

class ParCSRGraph {
public:
    void begin(uint32_t num_graphs) {
        graphs.clear();
        parallelism = num_graphs;
        vtx_dist = (idx_t *) malloc(sizeof(idx_t) * (num_graphs + 1));
        vtx_dist[0] = 0;
        current_ngraphs = 0;
        current_nvtxs = 0;
    }

    void add_graph(CSRGraph * graph) {
        assert(current_ngraphs < (parallelism + 1));
        graphs.push_back(graph);

        vtx_dist[(current_ngraphs + 1)] = current_nvtxs;
        current_ngraphs++;
        current_nvtxs += graph->nvtxs;
    }

    void finish() {
        assert(current_ngraphs == parallelism);
        assert(parallelism >= 2);
        idx_t num_vertices_per_part = vtx_dist[1];
        for(uint32_t i = 1; i < parallelism; i++) {
            assert((vtx_dist[i] - vtx_dist[i-1]) == num_vertices_per_part);
        }
    }

    CSRGraph * get_graph(uint32_t i) {
        return graphs[i];
    }

    uint32_t   get_num_graphs() {
        return parallelism;
    }
protected:
    vector<CSRGraph *>  graphs;
    idx_t *             vtx_dist;
    uint32_t            parallelism;
    uint64_t            current_nvtxs;
    uint32_t            current_ngraphs;

    friend class METISGraphPartitioner;
    friend class ParMETISGraphPartitioner;
};

class METISGraphPartitioner {
    idx_t       objval;
    idx_t       nparts;
    idx_t *     parts;
    idx_t *     options;
    CSRGraph *     graph;
public:
    void        partition       (CSRGraph * _graph, int num_partitions);
    void        partition       (ParCSRGraph * parGraph, int num_partitions);
    uint32_t    get_partition   (uint64_t vtx);
};

inline uint32_t METISGraphPartitioner::get_partition(uint64_t vtx) {
    assert((idx_t) vtx < graph->nvtxs);
    return static_cast<uint32_t>(parts[vtx]);
}

class ParMETISGraphPartitioner {
    idx_t *         obj_vals;
    idx_t *         vtx_dist; //should be multiples array
    idx_t           nparts;
    vector<idx_t *> parts;
    ParCSRGraph *   graphs;
    uint32_t        parallelism;
    uint32_t        num_vertices_per_processor;

public:
    void partition (ParCSRGraph * parGraph, int num_partitions);

    uint32_t get_partition (uint64_t vtx) {
        auto index = static_cast<int>(vtx / num_vertices_per_processor);
        auto offset = static_cast<int>(vtx % num_vertices_per_processor);
        return static_cast<uint32_t>(parts[index][offset]);
    }

protected:
    static void * partition_helper(void * data);
};

#endif //DBX1000_GRAPH_CREATOR_H
