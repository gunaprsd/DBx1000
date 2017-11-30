#include "graph_partitioner.h"

void GraphPartitioner::begin(uint32_t num_vertices) {
    graph           = new Graph();
    graph->nvtxs    = (idx_t) num_vertices;
    graph->ncon     = 1;
    graph->xadj     = (idx_t *) malloc(sizeof(idx_t) * (graph->nvtxs + 1));
    graph->vwgt     = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);

    cvtx = 0;
    cpos = 0;
    current_index   = 0;
    current_offset  = 0;

    init_batch_size     = graph->nvtxs * 2;
    current_batch_size  = init_batch_size;
    current_adjncy      = (idx_t *) malloc(sizeof(idx_t) * current_batch_size);
    current_adjwgt      = (idx_t *) malloc(sizeof(idx_t) * current_batch_size);
}

void GraphPartitioner::move_to_next_vertex() {
    graph->xadj[cvtx] = cpos;
    graph->vwgt[cvtx] = 1;
    cvtx++;
}

void GraphPartitioner::add_edge(uint32_t adj_vertex, uint32_t weight) {
    //Add edge into array
    current_adjncy[current_offset] = (idx_t) adj_vertex;
    current_adjwgt[current_offset] = (idx_t) weight;
    current_offset++;

    //Check if current array is over and reset it
    if(current_offset == current_batch_size) {
        //push current arrays into the vectors
        adjncies.push_back(current_adjncy);
        adjwgts.push_back(current_adjwgt);

        //update current fields
        current_index++;
        current_offset = 0;
        current_batch_size *= 2;
        //new array of size 2x
        current_adjncy = (idx_t *) malloc(sizeof(idx_t) * current_batch_size);
        current_adjwgt = (idx_t *) malloc(sizeof(idx_t) * current_batch_size);
    }

    //increment number of edges (double counting)
    cpos++;
}

void GraphPartitioner::finish() {
    assert(cvtx == graph->nvtxs);
    graph->xadj[cvtx] = cpos;
    graph->nedges = cpos / 2;
    graph->adjncy_size = cpos;
    prepare();
}

void GraphPartitioner::do_cluster(int num_clusters) {
    assert(graph != NULL);
    assert(graph->xadj != NULL);
    assert(graph->adjncy != NULL);
    assert(graph->vwgt != NULL);
    assert(graph->adjwgt != NULL);

    //Initiate number of partitions
    graph->nparts   = num_clusters;
    graph->ncon     = 1;
    
    //Create result locations
    graph->objval   = 0;
    graph->parts    = (idx_t *) malloc(sizeof(idx_t) * graph->nvtxs);
    for(uint32_t i = 0; i < graph->nvtxs; i++) {
      graph->parts[i] = -1;
    }
    
    //Create options
    graph->options  = (idx_t *) malloc(sizeof(idx_t) * METIS_NOPTIONS);
    METIS_SetDefaultOptions(graph->options);
    graph->options[METIS_OPTION_UFACTOR] = 1;
    graph->options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_VOL;
    int result = METIS_PartGraphKway(& graph->nvtxs,
                                     & graph->ncon,
                                     graph->xadj,
                                     graph->adjncy,
                                     graph->vwgt,
                                     NULL,
                                     graph->adjwgt,
                                     & graph->nparts,
                                     NULL,
                                     NULL,
                                     graph->options,
                                     & graph->objval,
                                     graph->parts);

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

void GraphPartitioner::prepare() {
    if(current_index > 0) {
        graph->adjncy = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);
        graph->adjwgt = (idx_t *) malloc(sizeof(idx_t) * graph->adjncy_size);

        idx_t main_array_ptr = 0;
        idx_t batch_size = init_batch_size;
        for(idx_t i = 0; i <= current_index; i++) {
            if(i < current_index) {
                //copy entire array into adjncy
                memcpy(graph->adjncy + main_array_ptr, adjncies[i], sizeof(idx_t) * batch_size);
                memcpy(graph->adjwgt + main_array_ptr, adjwgts[i], sizeof(idx_t) * batch_size);
                free(adjncies[i]);
                free(adjwgts[i]);
            } else {
                //copy only used part of array
                memcpy(graph->adjncy + main_array_ptr, current_adjncy, sizeof(idx_t) * current_offset);
                memcpy(graph->adjwgt + main_array_ptr, current_adjwgt, sizeof(idx_t) * current_offset);
                free(current_adjncy);
                free(current_adjwgt);
            }
            main_array_ptr += batch_size;
            batch_size *= 2;
        }
    } else {
        graph->adjncy = current_adjncy;
        graph->adjwgt = current_adjwgt;
    }


}

void GraphPartitioner::release() {
    graph->release();
    graph->reset();
}
