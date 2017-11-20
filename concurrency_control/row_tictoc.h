#pragma once 

#include "../system/Global.h"

#if CC_ALG == TICTOC

#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else 

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 63)
#define RTS_LEN (15)
#define WTS_LEN (63 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#endif

class TransactionManager;
class Row;

class Row_tictoc {
public:
	void 				initialize(Row * row);
	Status 					access(TransactionManager * txn, TimestampType type, Row * local_row);
#if SPECULATE
	Status					write_speculate(Row * data, Time version, bool spec_read);
#endif
	void				write_data(Row * data, Time wts);
	void				write_ptr(Row * data, Time wts, char *& data_to_free);
	bool 				renew_lease(Time wts, Time rts);
	bool 				try_renew(Time wts, Time rts, Time &new_rts, uint64_t thd_id);
	
	void 				lock();
	bool  				try_lock();
	void 				release();

	Time 				get_wts();
	Time 				get_rts();
	void 				get_ts_word(bool &lock, uint64_t &rts, uint64_t &wts);
private:
	Row * 			_row;
#if ATOMIC_WORD
	volatile uint64_t	_ts_word; 
#else
	Time 				_wts; // last write timestamp
	Time 				_rts; // end lease timestamp
	pthread_mutex_t * 	_latch;
#endif
#if TICTOC_MV
	volatile Time 		_hist_wts;
#endif
};

#endif
