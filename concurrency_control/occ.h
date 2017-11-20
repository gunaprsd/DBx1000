#pragma once 

#include "pthread.h"
#include "Types.h"
#include "Row.h"

class TransactionManager;

class RWSetEntry{
public:
	RWSetEntry();
	uint64_t tn;
	TransactionManager * txn;
	uint32_t set_size;
	Row ** rows;
	RWSetEntry * next;
};

class OCCManager {
public:
	void initialize();
	Status validate(TransactionManager * txn);
	volatile bool lock_all;
	uint64_t lock_txn_id;
private:

	Status 	per_row_validate		(TransactionManager * txn);
	Status 	central_validate		(TransactionManager * txn);
	bool 	test_valid			(RWSetEntry * set1, RWSetEntry * set2);
	Status 	get_rw_set(TransactionManager * txni, RWSetEntry * & rset, RWSetEntry * & wset);

	uint64_t 	his_len;
	uint64_t	 	active_len;

	RWSetEntry * 	history;
	RWSetEntry * 	active;

	volatile uint64_t 	tnc; // transaction number counter
	pthread_mutex_t 		latch;
};
