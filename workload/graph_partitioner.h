#include "global.h"
#include "metis.h"
#include "helper.h"
#include <stdlib.h>
#include <assert.h>


#ifndef DBX1000_GRAPH_CREATOR_H
#define DBX1000_GRAPH_CREATOR_H

class METISGraphPartitioner;
class ParMETISGraphPartitioner;
struct ParMETIS_CSRGraph;

struct METIS_CSRGraph {
    //Inputs to the partitioner
    idx_t nvtxs;
    idx_t ncon;
    idx_t adjncy_size;

    idx_t * xadj;
    idx_t * adjncy;
    idx_t * adjwgt;
    idx_t * vwgt;

    METIS_CSRGraph() {
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

struct ParMETIS_CSRGraph {
public:
    idx_t            ngraphs;
    idx_t            nvtxs_per_graph;

    METIS_CSRGraph * *  graphs;
    idx_t *             vtxdist;

    ParMETIS_CSRGraph() {
        ngraphs = 0;
        nvtxs_per_graph = 0;

        graphs = nullptr;
        vtxdist = nullptr;
    }

    void release() {
        for(auto i = 0; i < ngraphs; i++) {
            graphs[i]->release();
        }
        delete graphs;
        delete vtxdist;
    }

    friend class METISGraphPartitioner;
};

class METIS_CSRGraphCreator {
public:

    void begin(uint32_t num_vertices)
    {
        graph           = new METIS_CSRGraph();
        graph->ncon     = 1;
        graph->nvtxs    = (idx_t) num_vertices;
        graph->xadj     = (idx_t *) malloc(sizeof(idx_t) * (graph->nvtxs + 1));
        graph->vwgt     = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);

			for(idx_t i = 0; i < graph->nvtxs; i++) {
					graph->xadj[i] = -1;
					graph->vwgt[i] = -1;
				}
				graph->xadj[graph->nvtxs] = -1;

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

    METIS_CSRGraph * get_graph()
    {
        return graph;
    }

protected:
    idx_t   cvtx;
    idx_t   cpos;
    METIS_CSRGraph * graph;
    std::vector<idx_t> adjncies;
    std::vector<idx_t> adjwgts;

public:
    static METIS_CSRGraph * convert_METIS_CSRGraph(ParMETIS_CSRGraph *graph);
};

class ParMETIS_CSRGraphCreator {
public:
    void begin(uint32_t num_graphs) {

        parallelism = num_graphs;
        parGraph = new ParMETIS_CSRGraph();
        parGraph->graphs = new METIS_CSRGraph *[parallelism];
        parGraph->vtxdist = (idx_t *) malloc(sizeof(idx_t) * parallelism);

        current_ngraphs = 0;
        current_nvtxs = 0;
    }

    void add_graph(METIS_CSRGraph * graph) {
        assert(current_ngraphs < (parallelism + 1));
        parGraph->graphs[current_ngraphs] = graph;
        parGraph->vtxdist[(current_ngraphs + 1)] = current_nvtxs;
        current_ngraphs++;
        current_nvtxs += graph->nvtxs;
    }

    void finish() {
        assert(current_ngraphs == parallelism);
        assert(parallelism >= 2);
        for(uint32_t i = 1; i < parallelism; i++) {
            assert((parGraph->vtxdist[i] - parGraph->vtxdist[i-1]) == parGraph->nvtxs_per_graph);
        }
        parGraph->nvtxs_per_graph = parGraph->vtxdist[1];
    }

    ParMETIS_CSRGraph * get_graph() {
        return parGraph;
    }

protected:
    ParMETIS_CSRGraph * parGraph;

    uint32_t            parallelism;
    uint64_t            current_nvtxs;
    uint32_t            current_ngraphs;

    friend class METISGraphPartitioner;
    friend class ParMETISGraphPartitioner;
};

class METISGraphPartitioner {
public:
    static void compute_partitions(METIS_CSRGraph * graph, idx_t num_partitions, idx_t * parts);
};

class ParMETISGraphPartitioner {
public:
    static void compute_partitions(ParMETIS_CSRGraph * parGraph, idx_t nparts, idx_t * parts);
protected:
    static void * partition_helper(void * data);
};

#endif //DBX1000_GRAPH_CREATOR_H
