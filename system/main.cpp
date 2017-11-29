#include "global.h"
#include "config.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "tpcc.h"

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

    TPCCWorkloadGenerator * generator = new TPCCWorkloadGenerator();
    generator->initialize(16, 1024 * 1024, "tpcc_test");
    generator->generate();
    generator->release();

    TPCCWorkloadPartitioner * partitioner = new TPCCWorkloadPartitioner();
    partitioner->initialize(16, 128 * 1024, 1024, "tpcc_test");
    partitioner->partition();
    partitioner->release();

    return 0;
}