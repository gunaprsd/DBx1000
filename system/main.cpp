#include "global.h"
#include "config.h"
#include "mem_alloc.h"

#include "ycsb_database.h"
#include "ycsb_workload.h"

void parser(int argc, char * argv[]);

int main(int argc, char* argv[]) {
    parser(argc, argv);
    mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
    stats.init();

    YCSBWorkloadGenerator * workload = new YCSBWorkloadGenerator();
    workload->initialize(2, 1024 * 1024, NULL);
    workload->generate();
    return 0;
}