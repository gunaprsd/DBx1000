#pragma once 

#include "global.h"
#include "helper.h"

class Workload;
class ycsb_query;
class tpcc_query;
class experiment_query;

class base_query {
public:
	virtual void init(uint64_t thd_id, Workload * h_wl) = 0;
	uint64_t waiting_time;
	uint64_t part_num;
	uint64_t * part_to_access;
};

// All the querise for a particular thread.
class Query_thd {
public:
	void init(Workload * h_wl, int thread_id);
	base_query * get_next_query(); 
	int q_idx;
#if WORKLOAD == YCSB
	ycsb_query * queries;
#elif WORKLOAD == EXPERIMENT
	experiment_query * queries;
#else 
	tpcc_query * queries;
#endif
	char pad[CACHE_LINE_SIZE - sizeof(void *) - sizeof(int)];
	drand48_data buffer;
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init(Workload * h_wl);
	void init_per_thread(int thread_id);
	base_query * get_next_query(uint64_t thd_id); 
	
private:
	static void * threadInitQuery(void * This);

	Query_thd ** all_queries;
	Workload * _wl;
	static int _next_tid;
};
