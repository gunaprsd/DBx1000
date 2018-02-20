// Copyright[2017] <Guna Prasaad>
#include "graph_partitioner.h"
#include <cstring>

void METISGraphPartitioner::compute_partitions(METIS_CSRGraph *graph,
                                               idx_t nparts, idx_t *parts) {
  assert(graph != nullptr);
  assert(graph->nvtxs != -1);
  assert(graph->adjncy_size != -1);
  assert(graph->xadj != nullptr);
  assert(graph->adjncy != nullptr);
  assert(graph->vwgt != nullptr);
  assert(graph->adjwgt != nullptr);

  // Create result locations
  idx_t objval = 0;
  for (uint32_t i = 0; i < graph->nvtxs; i++) {
    parts[i] = -1;
  }

  // Create options
  auto options =
      reinterpret_cast<idx_t *>(malloc(sizeof(idx_t) * METIS_NOPTIONS));

  METIS_SetDefaultOptions(options);
  options[METIS_OPTION_PTYPE] = METIS_PTYPE_KWAY;
  options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT;
  options[METIS_OPTION_CTYPE] = METIS_CTYPE_RM;
  options[METIS_OPTION_RTYPE] = METIS_RTYPE_FM;
  options[METIS_OPTION_UFACTOR] = FLAGS_ufactor;
  options[METIS_OPTION_SEED] = FLAGS_seed;
  options[METIS_OPTION_DBGLVL] = METIS_DBG_TIME;
  options[METIS_OPTION_MINCONN] = 1;
  printf("ufactor: %u\n", FLAGS_ufactor);

  // Do the compute_partitions
  int result =
      METIS_PartGraphKway(&graph->nvtxs, &graph->ncon, graph->xadj,
                          graph->adjncy, graph->vwgt, nullptr, graph->adjwgt,
                          &nparts, nullptr, nullptr, options, &objval, parts);
  // PRINT_INFO(ld, "Partitioning-Obj-Value", objval);
  switch (result) {
  case METIS_ERROR_INPUT:
    printf("Error in input!\n");
    exit(0);
  case METIS_ERROR_MEMORY:
    printf("Memory issue!\n");
    exit(0);
  case METIS_ERROR:
    printf("Unknown error\n");
    exit(0);
  default:
    break;
  }
}

void ParMETISGraphPartitioner::compute_partitions(ParMETIS_CSRGraph *parGraph,
                                                  idx_t nparts, idx_t *parts) {
  // Convert to a serial format and use METIS
  auto graph = METIS_CSRGraphCreator::convert_METIS_CSRGraph(parGraph);
  METISGraphPartitioner::compute_partitions(graph, nparts, parts);
  return;

  // Cannot create a multi-threaded ParMETIS! :(
  assert(false);
  auto num_threads = static_cast<uint32_t>(parGraph->ngraphs);
  auto threads = new pthread_t[num_threads];
  auto data = new ThreadLocalData[num_threads];
  auto current_ptr = parts;
  for (uint32_t i = 0; i < num_threads; i++) {
    data[i].fields[0] = (uint64_t)i;
    data[i].fields[1] = (uint64_t)parGraph;
    data[i].fields[2] = (uint64_t)nparts;
    data[i].fields[3] = (uint64_t)current_ptr;
    pthread_create(&threads[i], nullptr, partition_helper,
                   reinterpret_cast<void *>(&data[i]));
    current_ptr += parGraph->nvtxs_per_graph;
  }

  for (uint32_t i = 0; i < num_threads; i++) {
    pthread_join(threads[i], nullptr);
  }

  delete[] threads;
  delete[] data;
}

// void *ParMETISGraphPartitioner::partition_helper(void *data) {
//  auto threadLocalData = reinterpret_cast<ThreadLocalData *>(data);
//  auto thread_id = (uint32_t)threadLocalData->fields[0];
//  auto parGraph = reinterpret_cast<ParMETIS_CSRGraph *>(
//                                         threadLocalData->fields[1]);
//  auto nparts = (idx_t) threadLocalData->fields[2];
//  auto parts = reinterpret_cast<idx_t *>(threadLocalData->fields[3]);
//
//  auto graph = parGraph->graphs[thread_id];
//  idx_t wgtflag = 3;
//  idx_t numflag = 0;
//  auto tpwgts = new real_t[nparts];
//  for (auto i = 0; i < nparts; ++i) {
//    tpwgts[i] = 1.0;
//  }
//
//  auto ubvec = new real_t[1];
//  ubvec[0] = 1.0 + ((real_t)g_ufactor/1000.0);
//  auto options = new idx_t[3];
//  options[0] = 0;  // default _wl_config
//  options[1] = 0;
//  options[2] = 123;
//
//  idx_t edgecut = 0;
//
//  int result = ParMETIS_V3_PartKway(parGraph->vtxdist,
//                              graph->xadj,
//                              graph->adjncy,
//                              graph->vwgt,
//                              graph->adjwgt,
//                              & wgtflag,
//                              & numflag,
//                              & graph->ncon,
//                              & nparts,
//                              tpwgts,
//                              ubvec,
//                              options,
//                              & edgecut,
//                              parts,
//                              nullptr);
//
//  switch (result) {
//    case METIS_ERROR_INPUT  :   printf("Error in input!\n");
//      exit(0);
//    case METIS_ERROR_MEMORY :   printf("Could not allocate required
//    memory!\n");
//      exit(0);
//    case METIS_ERROR        :   printf("Unknown error\n");
//      exit(0);
//    default                 :   break;
//  }
//
//  for (auto i = 0u; i < parGraph->nvtxs_per_graph; ++i) {
//    assert(parts[i] != -1);
//  }
//
//  delete options;
//  return nullptr;
//}

METIS_CSRGraph *
METIS_CSRGraphCreator::convert_METIS_CSRGraph(ParMETIS_CSRGraph *parGraph) {
  // Compute adjacency list size
  uint64_t num_vtxs = 0;
  uint64_t adjncy_size = 0;
  auto _parallelism = static_cast<uint64_t>(parGraph->ngraphs);
  for (uint32_t i = 0; i < _parallelism; i++) {
    adjncy_size += parGraph->graphs[i]->adjncy_size;
    num_vtxs += parGraph->graphs[i]->nvtxs;
  }

  // Initialize the parGraph
  auto graph = new METIS_CSRGraph();
  graph->ncon = 1;
  graph->nvtxs = num_vtxs;
  graph->adjncy_size = adjncy_size;

  graph->xadj =
      reinterpret_cast<idx_t *>(malloc(sizeof(idx_t) * (graph->nvtxs + 1)));
  graph->vwgt = reinterpret_cast<idx_t *>(malloc(sizeof(idx_t) * graph->nvtxs));
  graph->adjncy =
      reinterpret_cast<idx_t *>(malloc(sizeof(idx_t) * graph->adjncy_size));
  graph->adjwgt =
      reinterpret_cast<idx_t *>(malloc(sizeof(idx_t) * graph->adjncy_size));

  int vtx_array_offset = 0;
  int adj_array_offset = 0;
  for (uint32_t i = 0; i < _parallelism; i++) {
    METIS_CSRGraph *sgraph = parGraph->graphs[i];

    // Copy arrays that don't change
    memcpy(&graph->vwgt[vtx_array_offset], sgraph->vwgt,
           sizeof(idx_t) * sgraph->nvtxs);
    memcpy(&graph->adjncy[adj_array_offset], sgraph->adjncy,
           sizeof(idx_t) * sgraph->adjncy_size);
    memcpy(&graph->adjwgt[adj_array_offset], sgraph->adjwgt,
           sizeof(idx_t) * sgraph->adjncy_size);

    // Copy and adjust xadj array
    memcpy(&graph->xadj[vtx_array_offset], sgraph->xadj,
           sizeof(idx_t) * sgraph->nvtxs);
    for (uint32_t j = 0; j < sgraph->nvtxs; j++) {
      graph->xadj[j + vtx_array_offset] += adj_array_offset;
    }

    vtx_array_offset += sgraph->nvtxs;
    adj_array_offset += sgraph->adjncy_size;
  }
  graph->xadj[graph->nvtxs] = adj_array_offset;

  // Releasing memory held by all sub-graphs
  for (uint32_t i = 0; i < _parallelism; i++) {
    parGraph->graphs[i]->release();
  }

  return graph;
}
