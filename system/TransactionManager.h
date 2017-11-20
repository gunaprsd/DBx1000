#pragma once 
#ifndef __SYSTEM_TRANSACTION_MANAGER_H__
#define __SYSTEM_TRANSACTION_MANAGER_H__

#include "Global.h"
#include "Helper.h"
#include "Types.h"

class Workload;
class Thread;
class Row;
class Table;
class Query;
class INDEX;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_BLOCKED, VLL_FREE};

class Access {
public:
	AccessType 	type;
	Row * 	orig_row;
	Row * 	data;
	Row * 	orig_data;
	void cleanup();
#if CC_ALG == TICTOC
	Time 		wts;
	Time 		rts;
#elif CC_ALG == SILO
	Time 		tid;
	Time 		epoch;
#elif CC_ALG == HEKATON
	void * 		history_entry;	
#endif

};

class TransactionManager
{
public:
						TransactionManager() {}
	virtual 		 		~TransactionManager() {}

	ThreadId 			get_thread_id	();
	Workload * 			get_workload		();

	TxnId 				get_transaction_id	();
	Time 				get_timestamp		();
	void 				set_transaction_id	(TxnId txn_id);
	void 				set_timestamp		(Time timestamp);

	virtual void 		initialize		(Thread * h_thd, Workload * h_wl, PartId part_id);
	virtual Status 		execute			(Query * query) = 0;
	void 				cleanup			(Status rc);
	void 				release			();
	Status 				finish			(Status rc);


	Thread * 			thread;
	Workload * 			workload;
	Random * 			mrand;
	uint64_t 			abort_cnt;
	pthread_mutex_t		txn_lock;
	Row * volatile  		cur_row;

#if CC_ALG == HEKATON
	void * volatile history_entry;
#endif
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	bool volatile 	lock_abort; // forces another waiting txn to abort.
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready; 
	// [HSTORE]
	int volatile 	ready_part;

#if CC_ALG == TICTOC
	Time 			get_max_wts() 	{ return _max_wts; }
	void 			update_max_wts(Time max_wts);
	Time 			last_wts;
	Time 			last_rts;
#elif CC_ALG == SILO
	Time 			last_tid;
#endif
	
	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	uint32_t 		row_cnt;
	uint32_t	 			wr_cnt;
	Access **		accesses;
	int 			num_accesses_alloc;

	// For VLL
	TxnType 			vll_txn_type;
	Record *			index_read(INDEX * index, Key key, int part_id);
	void 			index_read(INDEX * index, Key key, int part_id, Record *& item);
	Row * 			get_row(Row * row, AccessType type);
protected:	
	void 			insert_row(Row * row, Table * table);
private:
	// insert rows
	uint64_t 		insert_cnt;
	Row * 			insert_rows[MAX_ROW_PER_TXN];
	TxnId 			txn_id;
	Time 			timestamp;

	bool 		_write_copy_ptr;
#if CC_ALG == TICTOC || CC_ALG == SILO
	bool 			_pre_abort;
	bool 			_validation_no_wait;
#endif
#if CC_ALG == TICTOC
	bool			_atomic_timestamp;
	Time 			_max_wts;
	// the following methods are defined in concurrency_control/tictoc.cpp
	Status				validate_tictoc();
#elif CC_ALG == SILO
	Time 			_cur_tid;
	Status				validate_silo();
#elif CC_ALG == HEKATON
	Status 				validate_hekaton(Status rc);
#endif
};

#endif
