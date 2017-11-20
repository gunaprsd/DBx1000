#pragma once 
#ifndef __SYSTEM_MANAGER_H__
#define __SYSTEM_MANAGER_H__

#include "Global.h"
#include "Helper.h"
#include "Types.h"

class Row;
class TransactionManager;

class Manager {
public:
	void 			init();
	// returns the next timestamp.
	Time			get_ts(uint64_t thread_id);

	// For MVCC. To calculate the min active ts in the system
	void 			add_ts(uint64_t thd_id, Time ts);
	Time 			get_min_ts(uint64_t tid = 0);

	// HACK! the following mutexes are used to model a centralized
	// lock/timestamp manager. 
 	void 			lock_row(Row * row);
	void 			release_row(Row * row);
	
	TransactionManager * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
	void 						set_txn_man(TransactionManager * txn);
	
	uint64_t 		get_epoch() { return *_epoch; };
	void 	 		update_epoch();
private:
	// for SILO
	volatile uint64_t * _epoch;		
	Time * 			_last_epoch_update_time;

	pthread_mutex_t ts_mutex;
	uint64_t *		timestamp;
	pthread_mutex_t mutexes[BUCKET_CNT];
	uint64_t 		hash(Row * row);
	Time volatile * volatile * volatile all_ts;
	TransactionManager ** 		_all_txns;
	// for MVCC 
	volatile Time	_last_min_ts_time;
	Time			_min_ts;
};

#endif
