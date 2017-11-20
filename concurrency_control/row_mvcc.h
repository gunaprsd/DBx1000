#pragma once

class Table;
class Catalog;
class TransactionManager;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.

#if CC_ALG == MVCC
struct WriteHisEntry {
	bool valid;		// whether the entry contains a valid version
	bool reserved; 	// when valid == false, whether the entry is reserved by a P_REQ 
	Time ts;
	Row * row;
};

struct ReqEntry {
	bool valid;
	TimestampType type; // P_REQ or R_REQ
	Time ts;
	TransactionManager * txn;
	Time time;
};


class Row_mvcc {
public:
	void initialize(Row * row);
	Status access(TransactionManager * txn, TimestampType type, Row * row);
private:
 	pthread_mutex_t * latch;
	volatile bool blatch;

	Row * _row;

	Status conflict(TimestampType type, Time ts, uint64_t thd_id = 0);
	void update_buffer(TransactionManager * txn, TimestampType type);
	void buffer_req(TimestampType type, TransactionManager * txn, bool served);

	// Invariant: all valid entries in _requests have greater ts than any entry in _write_history 
	Row * 		_latest_row;
	Time			_latest_wts;
	Time			_oldest_wts;
	WriteHisEntry * _write_history;
	// the following is a small optimization.
	// the timestamp for the served prewrite request. There should be at most one 
	// served prewrite request. 
	bool  			_exists_prewrite;
	Time 			_prewrite_ts;
	uint32_t 		_prewrite_his_id;
	Time 			_max_served_rts;

	// _requests only contains pending requests.
	ReqEntry * 		_requests;
	uint32_t 		_his_len;
	uint32_t 		_req_len;
	// Invariant: _num_versions <= 4
	// Invariant: _num_prewrite_reservation <= 2
	uint32_t 		_num_versions;
	
	// list = 0: _write_history
	// list = 1: _requests
	void double_list(uint32_t list);
	Row * reserveRow(Time ts, TransactionManager * txn);
};

#endif
