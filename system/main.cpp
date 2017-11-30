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

    uint32_t num_threads = 4;
    TPCCWorkloadGenerator * generator = new TPCCWorkloadGenerator();
    generator->initialize(num_threads, 32 * 1024, nullptr);
    generator->generate();


    for(uint32_t i = 0; i < num_threads; i++) {
        BaseQueryList * qlist = generator->get_queries_list(i);
        while(!qlist->done()) {
            ycsb_query * query = (ycsb_query *) qlist->next();
            assert(query->params.request_cnt <= MAX_REQ_PER_QUERY);
        }
    }

    TPCCWorkloadPartitioner * partitioner = new TPCCWorkloadPartitioner();
    partitioner->initialize(num_threads, 32 * 1024, 1024 , generator);
    partitioner->partition();
    partitioner->release();

    return 0;
}
