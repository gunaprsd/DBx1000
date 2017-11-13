#include <mm_malloc.h>
#include "global.h"
#include "Row.h"
#include "txn.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_hekaton.h"
#include "row_occ.h"
#include "row_tictoc.h"
#include "row_silo.h"
#include "row_vll.h"
#include "mem_alloc.h"
#include "manager.h"
#include "Table.h"

inline Table* 	Row::get_table()
{
	return table;
}

inline char* 	Row::get_data()
{
	return data;
}

inline Catalog* Row::get_schema()
{
	return get_table()->get_schema();
}

inline const char* Row::get_table_name()
{
	return get_table()->get_table_name();
}

inline uint64_t Row::get_tuple_size()
{
	return get_schema()->get_tuple_size();
}

inline uint64_t Row::get_field_cnt()
{
	return get_schema()->num_columns;
}


inline uint64_t Row::get_row_id()
{
	return _row_id;
}

inline uint64_t Row::get_part_id()
{
	return _part_id;
}

inline uint64_t Row::get_primary_key()
{
	return _primary_key;
}

inline void Row::set_primary_key(uint64_t key)
{
	_primary_key = key;
}


inline void	Row::set_data(char* data, uint64_t offset, uint64_t size)
{
	memcpy(&(this->data[offset]), data, size);
}

inline void 	Row::copy(Row * src)
{
	set_data(src->get_data(), src->get_tuple_size());
}

inline void 	Row::set_data(char * data, uint64_t size)
{
	set_data(data, 0, size);
}

inline void 	Row::set_value(ColumnId id, void * ptr)
{
	set_data((char*)ptr, GET_POS(id), GET_SIZE(id));
}

inline void 	Row::set_value(ColumnId id, void * ptr, int size)
{
	set_data((char*)ptr, GET_POS(id), size);
}

inline void 	Row::set_value(const char * col_name, void * ptr)
{
	set_value( GET_ID(col_name), ptr);
}

inline char* Row::get_value(ColumnId id)
{
	return &data[GET_POS(id)];
}

inline char* Row::get_value(char * col_name)
{
	return &data[ GET_POS( GET_ID(col_name) ) ];
}



inline void 	Row::set_value(ColumnId col_id, uint64_t value)
{
	set_value(col_id, & value);
}

inline void 	Row::set_value(ColumnId col_id, int64_t value)
{
	set_value(col_id, & value);
}

inline void 	Row::set_value(ColumnId col_id, uint32_t value)
{
	set_value(col_id, & value);
}

inline void 	Row::set_value(ColumnId col_id, int32_t value)
{
	set_value(col_id, & value);
}

inline void 	Row::set_value(ColumnId col_id, double value)
{
	set_value(col_id, & value);
}

inline void 	Row::get_value(ColumnId col_id, uint64_t & value)
{
	value = *(uint64_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, int64_t & value)
{
	value = *(int64_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, uint32_t & value)
{
	value = *(uint32_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, int32_t & value)
{
	value = *(int32_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, double & value)
{
	value = *(double*) & data[GET_POS(col_id)];
}


inline void 	Row::free_row()
{
	free(data);
}


inline Status Row::initialize(Table * host_table, uint64_t part_id, uint64_t row_id)
{
	table = host_table;
	_row_id = row_id;
	_part_id = part_id;
	Catalog * schema = host_table->get_schema();
	int tuple_size = schema->get_tuple_size();
	data = (char *) _mm_malloc(sizeof(char) * tuple_size, 64);
	return OK;
}

inline void Row::initialize(int size)
{
	data = (char *) _mm_malloc(size, 64);
}

Status Row::switch_schema(Table * host_table)
{
	this->table = host_table;
	return OK;
}

/**************************************************
 * Concurrency Control Based Function Definitions
 **************************************************/

inline void Row::initialize_manager(Row * row)
{
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    manager = (Row_lock *) mem_allocator.alloc(sizeof(Row_lock), _part_id);
#elif CC_ALG == TIMESTAMP
    manager = (Row_ts *) mem_allocator.alloc(sizeof(Row_ts), _part_id);
#elif CC_ALG == MVCC
    manager = (Row_mvcc *) _mm_malloc(sizeof(Row_mvcc), 64);
#elif CC_ALG == HEKATON
    manager = (Row_hekaton *) _mm_malloc(sizeof(Row_hekaton), 64);
#elif CC_ALG == OCC
    manager = (Row_occ *) mem_allocator.alloc(sizeof(Row_occ), _part_id);
#elif CC_ALG == TICTOC
	manager = (Row_tictoc *) _mm_malloc(sizeof(Row_tictoc), 64);
#elif CC_ALG == SILO
	manager = (Row_silo *) _mm_malloc(sizeof(Row_silo), 64);
#elif CC_ALG == VLL
    manager = (Row_vll *) mem_allocator.alloc(sizeof(Row_vll), _part_id);
#endif

#if CC_ALG != HSTORE
	manager->init(this);
#endif
}

inline Status Row::get_row(access_t type, txn_man * txn, Row *& row)
{
	Status rc = OK;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
	uint64_t thd_id = txn->get_thd_id();
	lock_t lt = (type == RD || type == SCAN)? LOCK_SH : LOCK_EX;
	#if CC_ALG == DL_DETECT
		uint64_t * txnids;
		int txncnt;
		rc = this->manager->lock_get(lt, txn, txnids, txncnt);
	#else
		rc = this->manager->lock_get(lt, txn);
	#endif

	if (rc == OK) {
		row = this;
	} else if (rc == Abort) {
	} else if (rc == WAIT) {
		ASSERT(CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT);
		uint64_t starttime = get_sys_clock();
	#if CC_ALG == DL_DETECT
		bool dep_added = false;
	#endif
		uint64_t endtime;
		txn->lock_abort = false;
		INC_STATS(txn->get_thd_id(), wait_cnt, 1);
		while (!txn->lock_ready && !txn->lock_abort) 
		{
		#if CC_ALG == WAIT_DIE
			continue;
		#elif CC_ALG == DL_DETECT
			uint64_t last_detect = starttime;
			uint64_t last_try = starttime;

			uint64_t now = get_sys_clock();
			if (now - starttime > g_timeout ) {
				txn->lock_abort = true;
				break;
			}
			if (g_no_dl) {
				PAUSE
				continue;
			}
			int ok = 0;
			if ((now - last_detect > g_dl_loop_detect) && (now - last_try > DL_LOOP_TRIAL)) {
				if (!dep_added) {
					ok = dl_detector.add_dep(txn->get_txn_id(), txnids, txncnt, txn->row_cnt);
					if (ok == 0)
						dep_added = true;
					else if (ok == 16)
						last_try = now;
				}
				if (dep_added) {
					ok = dl_detector.detect_cycle(txn->get_txn_id());
					if (ok == 16)  // failed to lock the deadlock detector
						last_try = now;
					else if (ok == 0) 
						last_detect = now;
					else if (ok == 1) {
						last_detect = now;
					}
				}
			} else 
				PAUSE
		#endif
		}

		if (txn->lock_ready) 
			rc = OK;
		else if (txn->lock_abort) { 
			rc = Abort;
			return_row(type, txn, NULL);
		}
		endtime = get_sys_clock();
		INC_TMP_STATS(thd_id, time_wait, endtime - starttime);
		row = this;
	}
	return rc;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == HEKATON 
	uint64_t thd_id = txn->get_thd_id();
	// For TIMESTAMP RD, a new copy of the row will be returned.
	// for MVCC RD, the version will be returned instead of a copy
	// So for MVCC RD-WR, the version should be explicitly copied.
	//row_t * newr = NULL;
  #if CC_ALG == TIMESTAMP
	// TODO. should not call malloc for each row read. Only need to call malloc once 
	// before simulation starts, like TicToc and Silo.
	txn->cur_row = (Row *) mem_allocator.alloc(sizeof(Row), this->get_part_id());
	txn->cur_row->initialize(get_table(), this->get_part_id());
  #endif

	// TODO need to initialize the table/catalog information.
	TsType ts_type = (type == RD)? R_REQ : P_REQ; 
	rc = this->manager->access(txn, ts_type, row);
	if (rc == OK ) {
		row = txn->cur_row;
	} else if (rc == WAIT) {
		uint64_t t1 = get_sys_clock();
		while (!txn->ts_ready)
			PAUSE
		uint64_t t2 = get_sys_clock();
		INC_TMP_STATS(thd_id, time_wait, t2 - t1);
		row = txn->cur_row;
	}
	if (rc != Abort) {
		row->table = get_table();
		assert(row->get_schema() == this->get_schema());
	}
	return rc;
#elif CC_ALG == OCC
	// OCC always make a local copy regardless of read or write
	txn->cur_row = (Row *) mem_allocator.alloc(sizeof(Row), get_part_id());
	txn->cur_row->initialize(get_table(), get_part_id());
	rc = this->manager->access(txn, R_REQ);
	row = txn->cur_row;
	return rc;
#elif CC_ALG == TICTOC || CC_ALG == SILO
	// like OCC, tictoc also makes a local copy for each read/write
	row->table = get_table();
	TsType ts_type = (type == RD)? R_REQ : P_REQ; 
	rc = this->manager->access(txn, ts_type, row);
	return rc;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	row = this;
	return rc;
#else
	assert(false);
#endif
}

// the "row" is the row read out in get_row(). 
// For locking based CC_ALG, the "row" is the same as "this". 
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be 
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
inline void Row::return_row(access_t type, txn_man * txn, Row * row) {
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
	assert (row == NULL || row == this || type == XP);
	if (ROLL_BACK && type == XP) {// recover from previous writes.
		this->copy(row);
	}
	this->manager->lock_release(txn);
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC 
	// for RD or SCAN or XP, the row should be deleted.
	// because all WR should be companied by a RD
	// for MVCC RD, the row is not copied, so no need to free. 
  #if CC_ALG == TIMESTAMP
	if (type == RD || type == SCAN) {
		row->free_row();
		mem_allocator.free(row, sizeof(Row));
	}
  #endif
	if (type == XP) {
		this->manager->access(txn, XP_REQ, row);
	} else if (type == WR) {
		assert (type == WR && row != NULL);
		assert (row->get_schema() == this->get_schema());
		Status rc = this->manager->access(txn, W_REQ, row);
		assert(rc == OK);
	}
#elif CC_ALG == OCC
	assert (row != NULL);
	if (type == WR)
		manager->write( row, txn->end_ts );
	row->free_row();
	mem_allocator.free(row, sizeof(Row));
	return;
#elif CC_ALG == TICTOC || CC_ALG == SILO
	assert (row != NULL);
	return;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	return;
#else 
	assert(false);
#endif
}

