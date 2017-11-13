#ifndef _VLL_H_
#define _VLL_H_

#include "../system/Global.h"
#include "../system/Helper.h"
#include "../system/Query.h"

class TransactionManager;

class TxnQEntry {
public:
	TxnQEntry * prev;
	TxnQEntry * next;
	TransactionManager * 	txn;
};

class VLLMan {
public:
	void init();
	void vllMainLoop(TransactionManager * next_txn, Query * query);
	// 	 1: txn is blocked
	//	 2: txn is not blocked. Can run.
	//   3: txn_queue is full. 
	int beginTxn(TransactionManager * txn, Query * query, TxnQEntry *& entry);
	void finishTxn(TransactionManager * txn, TxnQEntry * entry);
	void execute(TransactionManager * txn, Query * query);
private:
    TxnQEntry * 			_txn_queue;
    TxnQEntry * 			_txn_queue_tail;
	int 					_txn_queue_size;
	pthread_mutex_t 		_mutex;

	TxnQEntry * getQEntry();
	void returnQEntry(TxnQEntry * entry);
};

#endif
