#include "graph_partitioner.h"
#include "parmetis.h"

void METISGraphPartitioner::compute_partitions(METIS_CSRGraph * graph, idx_t nparts, idx_t * parts) {
    assert(graph != nullptr);
    assert(graph->xadj != nullptr);
    assert(graph->adjncy != nullptr);
    assert(graph->vwgt != nullptr);
    assert(graph->adjwgt != nullptr);


    //Create result locations
    idx_t objval   = 0;
    for(uint32_t i = 0; i < graph->nvtxs; i++) { parts[i] = -1; }

    //Create options
    auto options  = (idx_t *) malloc(sizeof(idx_t) * METIS_NOPTIONS);
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_UFACTOR] = g_ufactor;
    options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT;

    //Do the compute_partitions
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

void ParMETISGraphPartitioner::compute_partitions(ParMETIS_CSRGraph * parGraph, idx_t nparts, idx_t * parts) {
    auto num_threads = static_cast<uint32_t>(parGraph->ngraphs);
    pthread_t       threads [num_threads];
    ThreadLocalData data    [num_threads];
    for(uint32_t i = 0; i < num_threads; i++) {
      data[i].fields[0] = (uint64_t) i;
      data[i].fields[1] = (uint64_t) parGraph;
      data[i].fields[2] = (uint64_t) nparts;
      data[i].fields[3] = (uint64_t) (parts + parGraph->nvtxs_per_graph);
      pthread_create(& threads[i], nullptr, partition_helper, (void *) & data[i]);
    }

    for(uint32_t i = 0; i < num_threads; i++) {
      pthread_join(threads[i], nullptr);
    }
}

void *ParMETISGraphPartitioner::partition_helper(void *data) {
    auto threadLocalData = (ThreadLocalData *)data;
    auto thread_id = (uint32_t)threadLocalData->fields[0];
    auto parGraph = (ParMETIS_CSRGraph *)threadLocalData->fields[1];
    auto nparts = (idx_t) threadLocalData->fields[2];
    auto parts = (idx_t *) threadLocalData->fields[3];

    auto graph = parGraph->graphs[thread_id];
    idx_t wgtflag = 3;
    idx_t numflag = 0;
    auto options = new idx_t[3];
    options[0] = 0; //default config
    options[1] = 0;
    options[2] = 123;
    idx_t edgecut = 0;
    int result = ParMETIS_V3_PartKway(parGraph->vtxdist,
                              graph->xadj,
                              graph->adjncy,
                              graph->vwgt,
                              graph->adjwgt,
                              & wgtflag,
                              & numflag,
                              & graph->ncon,
                              & nparts,
                              NULL,
                              NULL,
                              options,
                              & edgecut,
                              parts,
                              nullptr);

    switch(result) {
    case METIS_ERROR_INPUT  :   printf("Error in input!\n");
      exit(0);
    case METIS_ERROR_MEMORY :   printf("Could not allocate required memory!\n");
      exit(0);
    case METIS_ERROR        :   printf("Unknown error\n");
      exit(0);
    default                 :   break;
    }

    delete options;

    return nullptr;
}

METIS_CSRGraph *METIS_CSRGraphCreator::convert_METIS_CSRGraph(ParMETIS_CSRGraph *parGraph) {
  //Compute adjacency list size
  uint64_t num_vtxs = 0;
  uint64_t adjncy_size = 0;
  auto _parallelism = static_cast<uint64_t>(parGraph->ngraphs);
  for(uint32_t i = 0; i < _parallelism; i++) {
    adjncy_size += parGraph->graphs[i]->adjncy_size;
    num_vtxs += parGraph->graphs[i]->nvtxs;
  }

  //Initialize the parGraph
  auto graph = new METIS_CSRGraph();
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
    METIS_CSRGraph * pseudo_sub_graph = parGraph->graphs[i];

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

  return graph;
}
