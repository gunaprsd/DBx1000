#include "Experiment.h"
#include "Test.h"
#include "TPCC.h"
#include "YCSB.h"
#include "Allocator.h"
#include "Global.h"
#include "Manager.h"
#include "plock.h"
#include "occ.h"
#include "Query.h"
#include "Thread.h"
#include "vll.h"

void * f(void *);

Thread ** m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);
	
	mem_allocator.initialize(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.initialize();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");
	Workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new YCSB_Workload; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case EXPERIMENT :
			m_wl = new experiment_wl; break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);
	}
	m_wl->initialize();
	printf("workload initialized!\n");
	
	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt - 1];
	m_thds = new Thread * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (Thread *) _mm_malloc(sizeof(Thread), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Scheduler *) _mm_malloc(sizeof(Scheduler), 64);
	if (WORKLOAD != TEST)
		query_queue->initialize(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.initialize();
#elif CC_ALG == OCC
	occ_man.initialize();
#elif CC_ALG == VLL
	vll_man.initialize();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->initialize(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));
	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();
	
	if (WORKLOAD != TEST) {
		printf("Total Runtime = %f secs\n", ((double)(endtime - starttime))/(1000 * 1000 * 1000));
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}
