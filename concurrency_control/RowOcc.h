#ifndef ROW_OCC_H
#define ROW_OCC_H

#include "Types.h"
#include "Global.h"

class Table;
class Catalog;
class TransactionManager;
struct TsReqEntry;

class RowOcc {
public:
	void initialize(Row * row);
	Status access(TransactionManager * txn, TimestampType type);
	bool	 validate(Time start_ts) {
		return (start_ts >= _last_wts);
	}
	void	 write(Row * data, Time ts) {
		_row->copy(data);
		if (PER_ROW_VALID) {
			assert(ts > _last_wts);
			_last_wts = ts;
		}
	}
	void latch() {
		pthread_mutex_lock( _latch );
	}
	void release(){
		pthread_mutex_unlock( _latch );
	}

private:
	Row * 				_row;
	pthread_mutex_t * 	_latch;
	bool 				_blatch;
	Time 				_last_wts;
};

#endif
