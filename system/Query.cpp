#include <sched.h>
#include "Expt_Query.h"
#include "TPCC_Helper.h"
#include "TPCC_Query.h"
#include "YCSB_Query.h"
#include "Table.h"
#include "Query.h"

#include "Allocator.h"
#include "Workload.h"

/*************************************
 * Scheduler Function Definitions
 *************************************/
int Scheduler::_next_tid;

void 
Scheduler::initialize(Workload * h_wl) {
	all_queries = new QueryQueue * [g_thread_cnt];
	_wl = h_wl;
	_next_tid = 0;
	

#if WORKLOAD == YCSB	
	YCSB_Query::calculateDenom();
#elif WORKLOAD == EXPERIMENT
	experiment_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpcc_buffer != NULL);
#endif
	int64_t begin = get_server_clock();
	pthread_t p_thds[g_thread_cnt - 1];
	for (uint32_t i = 0; i < g_thread_cnt - 1; i++) {
		pthread_create(&p_thds[i], NULL, InitializeQueryQueue, this);
	}
	InitializeQueryQueue(this);
	for (uint32_t i = 0; i < g_thread_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t end = get_server_clock();
	printf("Query Queue Init Time %f\n", 1.0 * (end - begin) / 1000000000UL);

//	uint64_t request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
// #if true
// 	ofstream fout;
// 	fout.open("graph.csv", fstream::trunc);
// 	for(uint32_t i = 0; i < g_thread_cnt; i++) {
// 		for(uint32_t j = 0; j < request_cnt; j++) {
// #if WORKLOAD == YCSB
// 			uint64_t current_tid = i * request_cnt + j;
// 			ycsb_query * current = &all_queries[i]->queries[j];
// 			fout << current_tid;
// 			for(uint32_t k = 0; k < g_thread_cnt; k++) {
// 				for(uint64_t l = 0; l < request_cnt; l++) {
// 					ycsb_query * other = &all_queries[k]->queries[l];
// 					//check whether there is any conflict
// 					bool conflict = false;
// 					for (UInt32 tmp1 = 0; tmp1 < g_req_per_query; tmp1 ++) {
// 						ycsb_request * req1 = &current->requests[tmp1];
// 						for (UInt32 tmp2 = 0; tmp2 < g_req_per_query; tmp2 ++) {
// 							ycsb_request * req2 = &other->requests[tmp2];
// 							if(req1->key == req2->key) {
// 								conflict = true;
// 								break;
// 							}
// 						}
// 						if(conflict)
// 							break;	
// 					}
// 					if(conflict)
// 					{
// 						uint64_t other_tid = k * request_cnt + l;
// 						fout << ", "  << other_tid; 
// 					}
// 				}
// 			}
// 			fout << endl;
// #elif WORKLOAD == EXPERIMENT
// #endif
// 		}
// 	}
// 	fout.close();
// 	printf("Written Graph File!\n");
// 	exit(0);
// #endif
}

void Scheduler::initialize_each_thread(ThreadId thread_id)
{
	all_queries[thread_id] = (QueryQueue *) _mm_malloc(sizeof(QueryQueue), 64);
	all_queries[thread_id]->initialize(_wl, thread_id);
}

Query * Scheduler::next(ThreadId thd_id)
{
	Query * query = all_queries[thd_id]->next();
	return query;
}

void * Scheduler::InitializeQueryQueue(void * This)
{
	Scheduler * query_queue = (Scheduler *)This;
	uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);
	set_affinity(tid);
	query_queue->initialize_each_thread(tid);
	return NULL;
}

/**********************************
 * QueryQueue Function Definitions
 **********************************/

#if WORKLOAD == YCSB

void QueryQueue::initialize(Workload * wl, ThreadId id)
{
	current = 0;
	uint64_t request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
	queries = (YCSB_Query *) mem_allocator.allocate(sizeof(YCSB_Query) * request_cnt, id);
	srand48_r(id + 1, &buffer);
	for (uint32_t qid = 0; qid < request_cnt; qid ++) {
		new(&queries[qid]) YCSB_Query();
		queries[qid].init(id, wl, this);
	}
}

#elif WORKLOAD == EXPERIMENT

void QueryQueue::initialize(Workload * h_wl, ThreadId thread_id)
{
	uint64_t request_cnt;
	current = 0;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
	queries = (experiment_query *) mem_allocator.allocate(sizeof(experiment_query) * request_cnt, thread_id);
	srand48_r(thread_id + 1, &buffer);
	for (uint32_t qid = 0; qid < request_cnt; qid ++) {
		new(&queries[qid]) experiment_query();
		queries[qid].initialize(thread_id, h_wl, this);
	}
}

#elif WORKLOAD == TPCC

void QueryQueue::initialize(Workload * h_wl, ThreadId thread_id)
{
	uint64_t request_cnt;
	current = 0;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
	queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
	for (uint32_t qid = 0; qid < request_cnt; qid ++) {
		new(&queries[qid]) tpcc_query();
		queries[qid].initialize(thread_id, h_wl);
	}
}

#endif

Query * QueryQueue::next()
{
	Query * query = &queries[current++];
	return query;
}
