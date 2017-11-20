#pragma once 
#ifndef __SYSTEM_THREAD_H__
#define __SYSTEM_THREAD_H__

#include "Global.h"
#include "Manager.h"
class Query;
class Workload;
class TransactionManager;

struct AbortBufferEntry {
	Time 		ready_time;
	Query * 		query;
};

class Thread {
public:
	ThreadId 	id;
	Workload * 	worload;

	ThreadId 	get_thread_id	() { return id; }
	uint64_t 	get_host_cid		() {	return _host_cid; }
	uint64_t 	get_cur_cid		() { return _cur_cid; }
	void 		set_host_cid		(uint64_t cid) { _host_cid = cid; }
	void 		set_cur_cid		(uint64_t cid) {_cur_cid = cid; }
	Time 		get_next_ts	();
	void 		initialize		(ThreadId id, Workload * workload);
	Status 		run				();
private:
	uint64_t 		_host_cid;
	uint64_t 		_cur_cid;
	Time 			_curr_ts;
	drand48_data 	_buffer;

	Status	 		runTest		(TransactionManager * txn);

	AbortBufferEntry * 	_abort_buffer;
	uint32_t 			_abort_buffer_size;
	uint32_t 			_abort_buffer_empty_slots;
	bool 				_abort_buffer_enable;
};

inline Time Thread::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager->get_ts(get_thread_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager->get_ts(get_thread_id());
		return _curr_ts;
	}
}

#endif
