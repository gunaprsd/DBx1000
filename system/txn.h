#pragma once 

#include "global.h"
#include "helper.h"

class Workload;
class thread_t;
class Row;
class Table;
class base_query;
class INDEX;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

class Access {
public:
	access_t 	type;
	Row * 	orig_row;
	Row * 	data;
	Row * 	orig_data;
	void cleanup();
#if CC_ALG == TICTOC
	ts_t 		wts;
	ts_t 		rts;
#elif CC_ALG == SILO
	ts_t 		tid;
	ts_t 		epoch;
#elif CC_ALG == HEKATON
	void * 		history_entry;	
#endif

};

class txn_man
{
public:
	virtual void init(thread_t * h_thd, Workload * h_wl, uint64_t part_id);
	void release();
	thread_t * h_thd;
	Workload * h_wl;
	myrand * mrand;
	uint64_t abort_cnt;

	virtual Status 		run_txn(base_query * m_query) = 0;
	uint64_t 		get_thd_id();
	Workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();

	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();

	pthread_mutex_t txn_lock;
	Row * volatile cur_row;
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
	Status 				finish(Status rc);
	void 			cleanup(Status rc);
#if CC_ALG == TICTOC
	ts_t 			get_max_wts() 	{ return _max_wts; }
	void 			update_max_wts(ts_t max_wts);
	ts_t 			last_wts;
	ts_t 			last_rts;
#elif CC_ALG == SILO
	ts_t 			last_tid;
#endif
	
	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	int 			row_cnt;
	int	 			wr_cnt;
	Access **		accesses;
	int 			num_accesses_alloc;

	// For VLL
	TxnType 		vll_txn_type;
	Record *		index_read(INDEX * index, KeyId key, int part_id);
	void 			index_read(INDEX * index, KeyId key, int part_id, Record *& item);
	Row * 		get_row(Row * row, access_t type);
protected:	
	void 			insert_row(Row * row, Table * table);
private:
	// insert rows
	uint64_t 		insert_cnt;
	Row * 		insert_rows[MAX_ROW_PER_TXN];
	txnid_t 		txn_id;
	ts_t 			timestamp;

	bool _write_copy_ptr;
#if CC_ALG == TICTOC || CC_ALG == SILO
	bool 			_pre_abort;
	bool 			_validation_no_wait;
#endif
#if CC_ALG == TICTOC
	bool			_atomic_timestamp;
	ts_t 			_max_wts;
	// the following methods are defined in concurrency_control/tictoc.cpp
	Status				validate_tictoc();
#elif CC_ALG == SILO
	ts_t 			_cur_tid;
	Status				validate_silo();
#elif CC_ALG == HEKATON
	Status 				validate_hekaton(Status rc);
#endif
};
