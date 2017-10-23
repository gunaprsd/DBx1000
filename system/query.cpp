#include <sched.h>
#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "experiment_query.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"

/*************************************************/
//     class Query_queue
/*************************************************/
int Query_queue::_next_tid;

void 
Query_queue::init(workload * h_wl) {
	all_queries = new Query_thd * [g_thread_cnt];
	_wl = h_wl;
	_next_tid = 0;
	

#if WORKLOAD == YCSB	
	ycsb_query::calculateDenom();
#elif WORKLOAD == EXPERIMENT
	experiment_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpcc_buffer != NULL);
#endif
	int64_t begin = get_server_clock();
	pthread_t p_thds[g_thread_cnt - 1];
	for (UInt32 i = 0; i < g_thread_cnt - 1; i++) {
		pthread_create(&p_thds[i], NULL, threadInitQuery, this);
	}
	threadInitQuery(this);
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

void 
Query_queue::init_per_thread(int thread_id) {	
	all_queries[thread_id] = (Query_thd *) _mm_malloc(sizeof(Query_thd), 64);
	all_queries[thread_id]->init(_wl, thread_id);
}

base_query * 
Query_queue::get_next_query(uint64_t thd_id) { 	
	base_query * query = all_queries[thd_id]->get_next_query();
	return query;
}

void *
Query_queue::threadInitQuery(void * This) {
	Query_queue * query_queue = (Query_queue *)This;
	uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);
	
	// set cpu affinity
	set_affinity(tid);

	query_queue->init_per_thread(tid);
	return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/

void 
Query_thd::init(workload * h_wl, int thread_id) {
	uint64_t request_cnt;
	q_idx = 0;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if WORKLOAD == YCSB	
	queries = (ycsb_query *) mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);
	srand48_r(thread_id + 1, &buffer);
#elif WORKLOAD == EXPERIMENT
	queries = (experiment_query *) mem_allocator.alloc(sizeof(experiment_query) * request_cnt, thread_id);
	srand48_r(thread_id + 1, &buffer);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_query();
		queries[qid].init(thread_id, h_wl, this);
#elif WORKLOAD == EXPERIMENT	
		new(&queries[qid]) experiment_query();
		queries[qid].init(thread_id, h_wl, this);
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
		queries[qid].init(thread_id, h_wl);
#endif
	}
}

base_query * 
Query_thd::get_next_query() {
	base_query * query = &queries[q_idx++];
	return query;
}
