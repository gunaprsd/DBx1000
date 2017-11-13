#pragma once 

#include "Global.h"

class Workload;
class Query;
class TransactionManager;

class Thread {
public:
	uint64_t _thd_id;
	Workload * _wl;

	uint64_t 	get_thd_id();

	uint64_t 	get_host_cid();
	void 	 	set_host_cid(uint64_t cid);

	uint64_t 	get_cur_cid();
	void 		set_cur_cid(uint64_t cid);

	void 		initialize(ThreadId thd_id, Workload * workload);
	// the following function must be in the form void* (*)(void*)
	// to run with pthread.
	// conversion is done within the function.
	Status 			run();
private:
	uint64_t 	_host_cid;
	uint64_t 	_cur_cid;
	Time 		_curr_ts;
	Time 		get_next_ts();

	Status	 		runTest(TransactionManager * txn);
	drand48_data buffer;

	// A restart buffer for aborted txns.
	struct AbortBufferEntry	{
		Time ready_time;
		Query * query;
	};
	AbortBufferEntry * _abort_buffer;
	int _abort_buffer_size;
	int _abort_buffer_empty_slots;
	bool _abort_buffer_enable;
};
