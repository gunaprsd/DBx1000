#pragma once 

class Table;
class Catalog;
class TransactionManager;
struct TsReqEntry;

#if CC_ALG==SILO
#define LOCK_BIT (1UL << 63)

class Row_silo {
public:
	void 				initialize(Row * row);
	Status 				access(TransactionManager * txn, TimestampType type, Row * local_row);
	
	bool				validate(Time tid, bool in_write_set);
	void				write(Row * data, uint64_t tid);
	
	void 				lock();
	void 				release();
	bool				try_lock();
	uint64_t 			get_tid();

	void 				assert_lock() {assert(_tid_word & LOCK_BIT); }
private:
#if ATOMIC_WORD
	volatile uint64_t	_tid_word;
#else
 	pthread_mutex_t * 	_latch;
	Time 				_tid;
#endif
	Row * 			_row;
};

#endif
