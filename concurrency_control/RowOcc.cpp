#include "Row.h"
#include "RowOcc.h"
#include "Types.h"
#include "Allocator.h"
#include "TransactionManager.h"

void RowOcc::initialize(Row * row)
{
	PartId pid = row->get_part_id();
	_latch = (pthread_mutex_t *) mem_allocator.allocate(sizeof(pthread_mutex_t), pid);
	pthread_mutex_init( _latch, NULL );
	_blatch = false;
	_last_wts = 0;
	_row = row;
}

Status RowOcc::access(TransactionManager * txn, TimestampType type)
{
	Status status = OK;
	pthread_mutex_lock( _latch );
	if (type == R_REQ) {
		if (txn->start_ts < _last_wts) {
			status = ABORT;
		} else {
			txn->cur_row->copy(_row);
			status = OK;
		}
	} else {
		assert(false);
	}
	pthread_mutex_unlock( _latch );
	return status;
}
