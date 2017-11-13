#ifndef ROW_LOCK_H
#define ROW_LOCK_H

struct LockEntry {
    LockType type;
    TransactionManager * txn;
	LockEntry * next;
	LockEntry * prev;
};

class Row_lock {
public:
	void init(Row * row);
	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    Status lock_get(LockType type, TransactionManager * txn);
    Status lock_get(LockType type, TransactionManager * txn, uint64_t* &txnids, int &txncnt);
    Status lock_release(TransactionManager * txn);
	
private:
    pthread_mutex_t * latch;
	bool blatch;
	
	bool 		conflict_lock(LockType l1, LockType l2);
	LockEntry * get_entry();
	void 		return_entry(LockEntry * entry);
	Row * _row;
    LockType lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
	
	// owners is a single linked list
	// waiters is a double linked list 
	// [waiters] head is the oldest txn, tail is the youngest txn. 
	//   So new txns are inserted into the tail.
	LockEntry * owners;	
	LockEntry * waiters_head;
	LockEntry * waiters_tail;
};

#endif
