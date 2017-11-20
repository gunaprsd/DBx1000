#include "occ.h"

#include "Allocator.h"
#include "Global.h"
#include "Helper.h"
#include "Manager.h"
#include "TransactionManager.h"
#include "RowOcc.h"


RWSetEntry::RWSetEntry() {
	set_size = 0;
	txn = NULL;
	rows = NULL;
	next = NULL;
}

void OCCManager::initialize() {
	tnc = 0;
	his_len = 0;
	active_len = 0;
	active = NULL;
	lock_all = false;
}

Status OCCManager::validate(TransactionManager * txn) {
	Status status;
#if PER_ROW_VALID
	status = per_row_validate(txn);
#else
	status = central_validate(txn);
#endif
	return status;
}

Status OCCManager::per_row_validate(TransactionManager * txn) {
	Status status = OK;
	// sort all rows accessed in primary key order.
	for (int i = txn->row_cnt - 1; i > 0; i--) {
		for (int j = 0; j < i; j ++) {
			int tabcmp = strcmp(txn->accesses[j]->orig_row->get_table_name(), 
			txn->accesses[j+1]->orig_row->get_table_name());
			if (tabcmp > 0 || (tabcmp == 0 && txn->accesses[j]->orig_row->get_primary_key() > txn->accesses[j+1]->orig_row->get_primary_key())) {
				Access * tmp = txn->accesses[j]; 
				txn->accesses[j] = txn->accesses[j+1];
				txn->accesses[j+1] = tmp;
			}
		}
	}

	// lock, validate each access and unlock
	bool ok = true;
	int lock_count = 0;
	for (uint32_t i = 0; i < txn->row_cnt && ok; i++) {
		lock_count ++;
		RowOcc* manager = txn->accesses[i]->orig_row->manager;
		manager->latch();
		ok = manager->validate(txn->start_ts);
	}

	if (ok) {
		txn->end_ts = glob_manager->get_ts( txn->get_thread_id() );
		txn->cleanup(OK);
		status = OK;
	} else {
		txn->cleanup(ABORT);
		status = ABORT;
	}

	for (int i = 0; i < lock_count; i++) {
		RowOcc* manager = txn->accesses[i]->orig_row->manager;
		manager->release();
	}
	return status;
}

Status OCCManager::central_validate(TransactionManager * txn) {
	Status status;

	uint64_t start_ts;
	uint64_t finish_ts;

	RWSetEntry * * 	finish_active;
	uint64_t 		f_active_len;

	bool valid = true;

	start_ts = txn->start_ts;
	RWSetEntry * wset = NULL;
	RWSetEntry * rset = NULL;
	get_rw_set(txn, rset, wset);

	bool readonly = (wset->set_size == 0);
	RWSetEntry * his;
	RWSetEntry * entry;
	int n = 0;

	pthread_mutex_lock( &latch );
	finish_ts = tnc;
	entry = active;
	f_active_len = active_len;
	finish_active = (RWSetEntry**) mem_allocator.allocate(sizeof(RWSetEntry *) * f_active_len, 0);
	while (entry != NULL) {
		finish_active[n++] = entry;
		entry = entry->next;
	}

	if ( !readonly ) {
		active_len ++;
		STACK_PUSH(active, wset);
	}

	his = history;
	pthread_mutex_unlock( &latch );

	if (finish_ts > start_ts) {
		while (his && his->tn > finish_ts) 
			his = his->next;
		while (his && his->tn > start_ts) {
			valid = test_valid(his, rset);
			if (!valid) 
				goto final;
			his = his->next;
		}
	}

	for (uint32_t i = 0; i < f_active_len; i++) {
		RWSetEntry * wact = finish_active[i];
		valid = test_valid(wact, rset);
		if (valid) {
			valid = test_valid(wact, wset);
		} if (!valid)
			goto final;
	}
final:
	if (valid) {
		txn->cleanup(OK);
	}
	mem_allocator.free(rset, sizeof(RWSetEntry));

	if (!readonly) {
		// only update active & tnc for non-readonly transactions
		pthread_mutex_lock( &latch );
		RWSetEntry * act = active;
		RWSetEntry * prev = NULL;
		while (act->txn != txn) {
			prev = act;
			act = act->next;
		}
		assert(act->txn == txn);
		if (prev != NULL) {
			prev->next = act->next;
		} else {
			active = act->next;
		}
		active_len --;
		if (valid) {
			if (history)
				assert(history->tn == tnc);
			tnc ++;
			wset->tn = tnc;
			STACK_PUSH(history, wset);
			his_len ++;
		}
		pthread_mutex_unlock( &latch );
	}

	if (valid) {
		status = OK;
	} else {
		txn->cleanup(ABORT);
		status = ABORT;
	}
	return status;
}

Status OCCManager::get_rw_set(TransactionManager * txn, RWSetEntry * &rset, RWSetEntry *& wset) {
	wset = (RWSetEntry*) mem_allocator.allocate(sizeof(RWSetEntry), 0);
	rset = (RWSetEntry*) mem_allocator.allocate(sizeof(RWSetEntry), 0);
	wset->set_size = txn->wr_cnt;
	rset->set_size = txn->row_cnt - txn->wr_cnt;
	wset->rows = (Row **) mem_allocator.allocate(sizeof(Row *) * wset->set_size, 0);
	rset->rows = (Row **) mem_allocator.allocate(sizeof(Row *) * rset->set_size, 0);
	wset->txn = txn;
	rset->txn = txn;

	uint32_t n = 0, m = 0;
	for (uint32_t i = 0; i < txn->row_cnt; i++) {
		if (txn->accesses[i]->type == WR)
			wset->rows[n ++] = txn->accesses[i]->orig_row;
		else 
			rset->rows[m ++] = txn->accesses[i]->orig_row;
	}

	assert(n == wset->set_size);
	assert(m == rset->set_size);
	return OK;
}

bool OCCManager::test_valid(RWSetEntry * set1, RWSetEntry * set2) {
	for (uint32_t i = 0; i < set1->set_size; i++)
		for (uint32_t j = 0; j < set2->set_size; j++) {
			if (set1->rows[i] == set2->rows[j]) {
				return false;
			}
		}
	return true;
}
