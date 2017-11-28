#include "global.h"
#include "config.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"

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

    YCSBWorkloadGenerator * generator = new YCSBWorkloadGenerator();
    generator->initialize(2, 8 * 1024, "ycsb_test");
    generator->generate();

    YCSBWorkloadPartitioner * partitioner = new YCSBWorkloadPartitioner();
    partitioner->initialize(2, 8 * 1024, 8 * 1024, "ycsb_test");
    partitioner->partition_workload();
    partitioner->finalize();
    return 0;
}