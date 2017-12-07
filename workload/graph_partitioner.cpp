#include "graph_partitioner.h"

void METISGraphPartitioner::partition(CSRGraph * _graph, int num_partitions) {
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
    options[METIS_OPTION_UFACTOR] = g_ufactor;
    options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT;

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

void METISGraphPartitioner::partition(ParCSRGraph * parGraph, int num_partitions) {

    //Compute adjacency list size
    uint64_t num_vtxs = 0;
    uint64_t adjncy_size = 0;
    uint64_t _parallelism = parGraph->get_num_graphs();
    for(uint32_t i = 0; i < _parallelism; i++) {
        adjncy_size += parGraph->graphs[i]->adjncy_size;
        num_vtxs += parGraph->graphs[i]->nvtxs;
    }

    //Initialize the parGraph
    auto graph = new CSRGraph();
    graph->ncon      = 1;
    graph->nvtxs     = num_vtxs;
    graph->adjncy_size = adjncy_size;

    graph->xadj      = (idx_t *) malloc(sizeof(idx_t) * (graph->nvtxs + 1));
    graph->vwgt      = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);
    graph->adjncy    = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);
    graph->adjwgt    = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);

    int vtx_array_offset = 0;
    int adj_array_offset = 0;
    for(uint32_t i = 0; i < _parallelism; i++) {
        CSRGraph * pseudo_sub_graph = parGraph->graphs[i];

        //Copy arrays that don't change
        memcpy(& graph->vwgt[vtx_array_offset], pseudo_sub_graph->vwgt, sizeof(idx_t) * pseudo_sub_graph->nvtxs);
        memcpy(& graph->adjncy[adj_array_offset], pseudo_sub_graph->adjncy, sizeof(idx_t) * pseudo_sub_graph->adjncy_size);
        memcpy(& graph->adjwgt[adj_array_offset], pseudo_sub_graph->adjwgt, sizeof(idx_t) * pseudo_sub_graph->adjncy_size);

        //Copy and adjust xadj array
        memcpy(& graph->xadj[vtx_array_offset], pseudo_sub_graph->xadj, sizeof(idx_t) * pseudo_sub_graph->nvtxs);
        for(uint32_t j = 0; j < pseudo_sub_graph->nvtxs; j++) {
            graph->xadj[j + vtx_array_offset] += adj_array_offset;
        }

        vtx_array_offset += pseudo_sub_graph->nvtxs;
        adj_array_offset += pseudo_sub_graph->adjncy_size;
    }
    graph->xadj[graph->nvtxs] = adj_array_offset;

    for(uint32_t i = 0; i < _parallelism; i++) { parGraph->graphs[i]->release(); }

    partition(graph, num_partitions);
}

void ParMETISGraphPartitioner::partition(ParCSRGraph *parGraph, int num_partitions) {

}

void *ParMETISGraphPartitioner::partition_helper(void *data) {
    return nullptr;
}
