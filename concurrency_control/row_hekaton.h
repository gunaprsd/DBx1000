#pragma once
#include "row_mvcc.h"

class Table;
class Catalog;
class TransactionManager;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.

#if CC_ALG == HEKATON

struct WriteHisEntry {
	bool begin_txn;	
	bool end_txn;
	Time begin;
	Time end;
	Row * row;
};

#define INF UINT64_MAX

class Row_hekaton {
public:
	void 			initialize(Row * row);
	Status 			access(TransactionManager * txn, TimestampType type, Row * row);
	Status 			prepare_read(TransactionManager * txn, Row * row, Time commit_ts);
	void 			post_process(TransactionManager * txn, Time commit_ts, Status rc);

private:
	volatile bool 	blatch;
	uint32_t 		reserveRow(TransactionManager * txn);
	void 			doubleHistory();

	uint32_t 		_his_latest;
	uint32_t 		_his_oldest;
	WriteHisEntry * _write_history; // circular buffer
	bool  			_exists_prewrite;
	
	uint32_t 		_his_len;
};

#endif
