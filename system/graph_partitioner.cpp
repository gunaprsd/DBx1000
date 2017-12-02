#include "graph_partitioner.h"

void CSRGraphCreator::begin(uint32_t num_vertices) {
    graph           = new Graph();
    graph->ncon     = 1;
    graph->nvtxs    = (idx_t) num_vertices;
    graph->xadj     = (idx_t *) malloc(sizeof(idx_t) * (graph->nvtxs + 1));
    graph->vwgt     = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);

    cvtx = 0;
    cpos = 0;
}

void CSRGraphCreator::move_to_next_vertex(idx_t vwgt) {
    graph->xadj[cvtx] = cpos;
    graph->vwgt[cvtx] = 1;
    cvtx++;
}

void CSRGraphCreator::add_edge(uint64_t adj_vertex, int weight) {
    adjncies.push_back((idx_t)adj_vertex);
    adjwgts.push_back((idx_t)weight);
    cpos++;
}

void CSRGraphCreator::finish() {
    assert(cvtx == graph->nvtxs);
    graph->xadj[cvtx] = cpos;
    
    graph->adjncy_size = adjncies.size();;
    graph->adjncy   = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);
    graph->adjwgt   = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);

    int i = 0;
    for(auto iter = adjncies.begin(); iter != adjncies.end(); iter++) {
        graph->adjncy[i] = *iter;
        i++;
    }

    i = 0;
    for(auto iter = adjwgts.begin(); iter != adjwgts.end(); iter++) {
        graph->adjwgt[i] = *iter;
        i++;
    }

    adjncies.clear();
    adjwgts.clear();
}


void METISGraphPartitioner::partition(Graph * _graph, int num_partitions) {
    assert(_graph != nullptr);
    assert(_graph->xadj != nullptr);
    assert(_graph->adjncy != nullptr);
    assert(_graph->vwgt != nullptr);
    assert(_graph->adjwgt != nullptr);

    graph     = _graph;

    //Create result locations
    nparts   = num_partitions;
    objval   = 0;
    parts    = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);
    for(uint32_t i = 0; i < graph->nvtxs; i++) { parts[i] = -1; }

    //Create options
    options  = (idx_t *) malloc(sizeof(idx_t) * METIS_NOPTIONS);
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_UFACTOR] = 1;
    options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_VOL;

    //Do the partition
    int result = METIS_PartGraphKway(& graph->nvtxs,
                                     & graph->ncon,
                                     graph->xadj,
                                     graph->adjncy,
                                     graph->vwgt,
                                     nullptr,
                                     graph->adjwgt,
                                     & nparts,
                                     nullptr,
                                     nullptr,
                                     options,
                                     & objval,
                                     parts);

    switch(result) {
        case METIS_ERROR_INPUT  :   printf("Error in input!\n");
        exit(0);
        case METIS_ERROR_MEMORY :   printf("Could not allocate required memory!\n");
        exit(0);
        case METIS_ERROR        :   printf("Unknown error\n");
        exit(0);
        default                 :   break;
    }
}
