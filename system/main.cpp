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

    ParallelWorkloadGenerator * generator = nullptr;
    WorkloadPartitioner * partitioner = nullptr;

    generator = new YCSBWorkloadGenerator();
    partitioner = new YCSBWorkloadPartitioner();
    uint32_t num_threads = 4;
    uint64_t num_params_per_thread = 1024;
    uint64_t num_params_pgpt = 128;

    generator->initialize(num_threads, num_params_per_thread, nullptr);
    generator->generate();

    partitioner->initialize(num_threads, num_params_per_thread, num_params_pgpt, generator);
    partitioner->partition();
    partitioner->release();

    return 0;
}
