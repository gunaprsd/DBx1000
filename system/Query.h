#pragma once 
#ifndef __SYSTEM_QUERY_H__
#define __SYSTEM_QUERY_H__

#include "Global.h"
#include "Helper.h"

class Workload;
class YCSB_Query;
class TPCCQuery;
class ExperimentQuery;

class Query
{
public:
	/* Constructors/Destructors */
				 	 Query	() {}
	virtual 			 ~Query	() {}

	/* Initializers */
	virtual void 	initialize(ThreadId id, Workload * wl) = 0;

	/* Data Fields */
	uint64_t 	waiting_time;
	uint64_t 	part_num;
	uint64_t * 	part_to_access;
};

class QueryQueue
{
public:
	/* Initializers */
	void 		initialize(Workload * h_wl, ThreadId thread_id);

	/* Accessor Functions */
	Query * 		next	();

	/* Data Fields */
	uint32_t current;
#if WORKLOAD == YCSB
	YCSB_Query * queries;
#elif WORKLOAD == EXPERIMENT
	ExperimentQuery * queries;
#else 
	TPCCQuery * queries;
#endif
	char padding[CACHE_LINE_SIZE - sizeof(void *) - sizeof(uint32_t)];
	drand48_data buffer;
};

class Scheduler
{
public:
	/* Initializers */
	void initialize(Workload * h_wl);
	void initialize_each_thread(ThreadId thread_id);

	/* Per-Thread Accessor Functions */
	Query * next(ThreadId thd_id);
	
private:
	static void * InitializeQueryQueue(void * This);

	QueryQueue ** 	all_queries;
	Workload * 		_wl;
	static int 		_next_tid;
};

#endif
