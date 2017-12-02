#include "global.h"
#include "config.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "ycsb.h"

void parser(int argc, char * argv[]);

int main(int argc, char* argv[]) {
    parser(argc, argv);
    mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
    stats.init();

    //Initialize global manager
    glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
    glob_manager->init();

    //CC protocol specific initializations
#if CC_ALG == DL_DETECT
    dl_detector.init();
#elif CC_ALG == HSTORE
    part_lock_man.init();
#elif CC_ALG == OCC
    occ_man.init();
#elif CC_ALG == VLL
    vll_man.init();
#endif

    auto generator = new YCSBWorkloadGenerator();
    generator->initialize(g_thread_cnt, MAX_TXN_PER_PART, nullptr);
    generator->generate();

    auto partitioner = new YCSBWorkloadPartitioner();
    partitioner->initialize(generator->get_queries_matrix(), MAX_NODES_FOR_CLUSTERING, INIT_PARALLELISM);
    partitioner->partition();
    partitioner->write_to_files("ycsb_test");
	  partitioner->print_execution_summary();

    return 0;
}
