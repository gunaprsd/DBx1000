#pragma once 
#ifndef __SYSTEM_STATISTICS_H__
#define __SYSTEM_STATISTICS_H__

#include "Global.h"
#include "Helper.h"



class ThreadStatistics {
public:
	void initialize(ThreadId thd_id);
	void clear();

	char _pad2[CACHE_LINE_SIZE];
	uint64_t txn_cnt;
	uint64_t abort_cnt;
	double run_time;
	double time_man;
	double time_index;
	double time_wait;
	double time_abort;
	double time_cleanup;
	uint64_t time_ts_alloc;
	double time_query;
	uint64_t wait_cnt;
	uint64_t debug1;
	uint64_t debug2;
	uint64_t debug3;
	uint64_t debug4;
	uint64_t debug5;
	
	uint64_t latency;
	uint64_t * all_debug1;
	uint64_t * all_debug2;
	char _pad[CACHE_LINE_SIZE];
};

class ThreadMonitor {
public:
	void init();
	void clear();
	double time_man;
	double time_index;
	double time_wait;
	char _pad[CACHE_LINE_SIZE - sizeof(double)*3];
};

class Statistics {
public:
	// PER THREAD statistics
	ThreadStatistics ** _stats;
	// stats are first written to tmp_stats, if the txn successfully commits, 
	// copy the values in tmp_stats to _stats
	ThreadMonitor ** _monitors;
	
	// GLOBAL statistics
	double dl_detect_time;
	double dl_wait_time;
	uint64_t cycle_detect;
	uint64_t deadlock;	

	void initialize();
	void initialize(ThreadId thread_id);
	void clear(ThreadId tid);
	void add_debug(ThreadId thd_id, uint64_t value, uint32_t select);
	void commit(ThreadId thd_id);
	void abort(ThreadId thd_id);
	void print();
	void print_lat_distr();
};
#endif
