#include "Row.h"
#include "row_occ.h"

#include "../system/Allocator.h"
#include "../system/TransactionManager.h"

void 
Row_occ::init(Row * row) {
	_row = row;
	int part_id = row->get_part_id();
	_latch = (pthread_mutex_t *) 
		mem_allocator.allocate(sizeof(pthread_mutex_t), part_id);
	pthread_mutex_init( _latch, NULL );
	wts = 0;
	blatch = false;
}

Status
Row_occ::access(TransactionManager * txn, TimestampType type) {
	Status rc = OK;
	pthread_mutex_lock( _latch );
	if (type == R_REQ) {
		if (txn->start_ts < wts)
			rc = Abort;
		else { 
			txn->cur_row->copy(_row);
			rc = OK;
		}
	} else 
		assert(false);
	pthread_mutex_unlock( _latch );
	return rc;
}

void
Row_occ::latch() {
	pthread_mutex_lock( _latch );
}

bool
Row_occ::validate(uint64_t ts) {
	if (ts < wts) return false;
	else return true;
}

void
Row_occ::write(Row * data, uint64_t ts) {
	_row->copy(data);
	if (PER_ROW_VALID) {
		assert(ts > wts);
		wts = ts;
	}
}

void
Row_occ::release() {
	pthread_mutex_unlock( _latch );
}
