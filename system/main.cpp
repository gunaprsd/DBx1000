#include "global.h"
#include "config.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "ycsb.h"

void parser(int argc, char * argv[]);

void pre_processing() {
    check_and_init_variables();
    ParallelWorkloadGenerator * generator = nullptr;
    ParallelWorkloadLoader * loader = nullptr;
    ParallelWorkloadPartitioner * partitioner = nullptr;
    BenchmarkExecutor * executor = nullptr;

    if(strcmp(g_benchmark, "ycsb") == 0) {
        generator = new YCSBWorkloadGenerator();
        loader = new YCSBWorkloadLoader();
        partitioner = new YCSBWorkloadPartitioner();
        executor = new YCSBExecutor();
    } else if(strcmp(g_benchmark, "tpcc") == 0) {
        generator = new TPCCWorkloadGenerator();
        loader = new TPCCWorkloadLoader();
        partitioner = new TPCCWorkloadPartitioner();
        executor = new TPCCExecutor();
    }

    if(g_task_type == PARTITION) {
        loader->initialize(g_thread_cnt, get_benchmark_path(false));
        loader->load();

        partitioner->initialize(loader->get_queries_matrix(), g_max_nodes_for_clustering, INIT_PARALLELISM);
        partitioner->partition();
        partitioner->write_to_files(get_benchmark_path(true));
        partitioner->print_execution_summary();

        loader->release();
    } else if(g_task_type == GENERATE){

        generator->initialize(g_thread_cnt, g_queries_per_thread, get_benchmark_path(false));
        generator->generate();
        generator->release();
    } else if(g_task_type == EXECUTE) {
        loader->initialize(g_thread_cnt, get_benchmark_path(true));
        loader->load();

        executor->initialize(g_thread_cnt, get_benchmark_path(true));
        executor->execute();

        executor->release();
        loader->release();
    }
}

int main(int argc, char* argv[]) {
    parser(argc, argv);

    pre_processing();

    return 0;
}
