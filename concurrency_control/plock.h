#ifndef _PLOCK_H_
#define _PLOCK_H_

#include "../system/Global.h"
#include "../system/Helper.h"

class TransactionManager;

// Parition manager for HSTORE
class PartMan {
public:
	void init();
	Status lock(TransactionManager * txn);
	void unlock(TransactionManager * txn);
private:
	pthread_mutex_t latch;
	TransactionManager * owner;
	TransactionManager ** waiters;
	UInt32 waiter_cnt;
};

// Partition Level Locking
class Plock {
public:
	void init();
	// lock all partitions in parts
	Status lock(TransactionManager * txn, uint64_t * parts, uint64_t part_cnt);
	void unlock(TransactionManager * txn, uint64_t * parts, uint64_t part_cnt);
private:
	PartMan ** part_mans;
};

#endif
