#include "Row.h"
#include "row_hekaton.h"
#include <mm_malloc.h>

#include "../system/Allocator.h"
#include "../system/Manager.h"
#include "../system/TransactionManager.h"

#if CC_ALG == HEKATON

void Row_hekaton::init(Row * row) {
	_his_len = 4;

	_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
	for (uint32_t i = 0; i < _his_len; i++) 
		_write_history[i].row = NULL;
	_write_history[0].row = row;
	_write_history[0].begin_txn = false;
	_write_history[0].end_txn = false;
	_write_history[0].begin = 0;
	_write_history[0].end = INF;

	_his_latest = 0;
	_his_oldest = 0;
	_exists_prewrite = false;

	blatch = false;
}

void 
Row_hekaton::doubleHistory()
{
	WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
	uint32_t idx = _his_oldest; 
	for (uint32_t i = 0; i < _his_len; i++) {
		temp[i] = _write_history[idx]; 
		idx = (idx + 1) % _his_len;
		temp[i + _his_len].row = NULL;
		temp[i + _his_len].begin_txn = false;
		temp[i + _his_len].end_txn = false;
	}

	_his_oldest = 0;
	_his_latest = _his_len - 1; 
	_mm_free(_write_history);
	_write_history = temp;

	_his_len *= 2;
}

Status Row_hekaton::access(TransactionManager * txn, TimestampType type, Row * row) {
	Status rc = OK;
	Time ts = txn->get_timestamp();
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	assert(_write_history[_his_latest].end == INF || _write_history[_his_latest].end_txn);
	if (type == R_REQ) {
		if (ISOLATION_LEVEL == REPEATABLE_READ) {
			rc = OK;
			txn->cur_row = _write_history[_his_latest].row;
		} else if (ts < _write_history[_his_oldest].begin) { 
			rc = ABORT;
		} else if (ts > _write_history[_his_latest].begin) {
			// TODO. should check the next history entry. If that entry is locked by a preparing txn,
			// may create a commit dependency. For now, I always return non-speculative entries.
			rc = OK;
			txn->cur_row = _write_history[_his_latest].row;
		} else {
			rc = OK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t i = _his_latest;
			bool find = false;
			while (true) {
				i = (i == 0)? _his_len - 1 : i - 1;
				if (_write_history[i].begin < ts) {
					assert(_write_history[i].end > ts);
					txn->cur_row = _write_history[i].row;
					find = true;
					break;
				}
				if (i == _his_oldest)
					break;
			}
			assert(find);
		}
	} else if (type == P_REQ) {
		if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
			rc = ABORT;
		} else {
			rc = OK;
			_exists_prewrite = true;
			uint32_t id = reserveRow(txn);
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
			_write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_transaction_id();
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_transaction_id();
			Row * res_row = _write_history[id].row;
			assert(res_row);
			res_row->copy(_write_history[_his_latest].row);
			txn->cur_row = res_row;
		}
	} else 
		assert(false);

	blatch = false;
	return rc;
}

uint32_t 
Row_hekaton::reserveRow(TransactionManager * txn)
{
	// Garbage Collection
	uint32_t idx;
	Time min_ts = glob_manager->get_min_ts(txn->get_thread_id());
	if ((_his_latest + 1) % _his_len == _his_oldest // history is full
		&& min_ts > _write_history[_his_oldest].end) 
	{
		while (_write_history[_his_oldest].end < min_ts) 
		{
			assert(_his_oldest != _his_latest);
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}
	
	if ((_his_latest + 1) % _his_len != _his_oldest) 
		// _write_history is not full, return the next entry.
		idx = (_his_latest + 1) % _his_len;
	else { 
		// write_history is already full
		// If _his_len is small, double it. 
		if (_his_len < g_thread_cnt) {
			doubleHistory();
			idx = (_his_latest + 1) % _his_len;
		} else {
			// _his_len is too large, should replace the oldest history
			idx = _his_oldest;
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}

	// some entries are not taken. But the row of that entry is NULL.
	if (!_write_history[idx].row) {
		_write_history[idx].row = (Row *) _mm_malloc(sizeof(Row), 64);
		_write_history[idx].row->initialize(MAX_TUPLE_SIZE);
	}
	return idx;
}

Status Row_hekaton::prepare_read(TransactionManager * txn, Row * row, Time commit_ts)
{
	Status rc;
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	// TODO may pass in a pointer to the history entry to reduce the following scan overhead.
	uint32_t idx = _his_latest;
	while (true) {
		if (_write_history[idx].row == row) {
			if (txn->get_timestamp() < _write_history[idx].begin) {
				rc = ABORT;
			} else if (!_write_history[idx].end_txn && _write_history[idx].end > commit_ts) 
				rc = OK;
			else if (!_write_history[idx].end_txn && _write_history[idx].end < commit_ts) {
				rc = ABORT;
			} else { 
				// TODO. if the end is a txn id, should check that status of that txn.
				// but for simplicity, we just commit
				rc = OK;
			}
			break;
		}
		if (idx == _his_oldest) {
			rc = ABORT;
			break;
		}
		idx = (idx == 0)? _his_len - 1 : idx - 1;
	}
	blatch = false;
	return rc;
}

void
Row_hekaton::post_process(TransactionManager * txn, Time commit_ts, Status rc)
{
	while (!ATOM_CAS(blatch, false, true))
		PAUSE

	WriteHisEntry * entry = &_write_history[ (_his_latest + 1) % _his_len ];
	assert(entry->begin_txn && entry->begin == txn->get_transaction_id());
	_write_history[ _his_latest ].end_txn = false;
	_exists_prewrite = false;
	if (rc == OK) {
		assert(commit_ts > _write_history[_his_latest].begin);
		_write_history[ _his_latest ].end = commit_ts;
		entry->begin = commit_ts;
		entry->end = INF;
		_his_latest = (_his_latest + 1) % _his_len;
		assert(_his_latest != _his_oldest);
	} else 
		_write_history[ _his_latest ].end = INF;
	
	blatch = false;
}

#endif
