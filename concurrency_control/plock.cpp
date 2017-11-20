#include "plock.h"

#include "../system/Allocator.h"
#include "../system/Global.h"
#include "../system/Helper.h"
#include "../system/TransactionManager.h"

/************************************************/
// per-partition Manager
/************************************************/
void PartMan::init() {
	uint64_t part_id = get_part_id(this);
	waiter_cnt = 0;
	owner = NULL;
	waiters = (TransactionManager **)
		mem_allocator.allocate(sizeof(TransactionManager *) * g_thread_cnt, part_id);
	pthread_mutex_init( &latch, NULL );
}

Status PartMan::lock(TransactionManager * txn) {
	Status rc;

	pthread_mutex_lock( &latch );
	if (owner == NULL) {
		owner = txn;
		rc = OK;
	} else if (owner->get_timestamp() < txn->get_timestamp()) {
		int i;
		assert(waiter_cnt < g_thread_cnt);
		for (i = waiter_cnt; i > 0; i--) {
			if (txn->get_timestamp() > waiters[i - 1]->get_timestamp()) {
				waiters[i] = txn;
				break;
			} else 
				waiters[i] = waiters[i - 1];
		}
		if (i == 0)
			waiters[i] = txn;
		waiter_cnt ++;
		ATOM_ADD(txn->ready_part, 1);
		rc = WAIT;
	} else
		rc = ABORT;
	pthread_mutex_unlock( &latch );
	return rc;
}

void PartMan::unlock(TransactionManager * txn) {
	pthread_mutex_lock( &latch );
	if (txn == owner) {		
		if (waiter_cnt == 0) 
			owner = NULL;
		else {
			owner = waiters[0];			
			for (uint32_t i = 0; i < waiter_cnt - 1; i++) {
				assert( waiters[i]->get_timestamp() < waiters[i + 1]->get_timestamp() );
				waiters[i] = waiters[i + 1];
			}
			waiter_cnt --;
			ATOM_SUB(owner->ready_part, 1);
		} 
	} else {
		bool find = false;
		for (uint32_t i = 0; i < waiter_cnt; i++) {
			if (waiters[i] == txn) 
				find = true;
			if (find && i < waiter_cnt - 1) 
				waiters[i] = waiters[i + 1];
		}
		ATOM_SUB(txn->ready_part, 1);
		assert(find);
		waiter_cnt --;
	}
	pthread_mutex_unlock( &latch );
}

/************************************************/
// Partition Lock
/************************************************/

void Plock::initialize() {
	ARR_PTR(PartMan, part_mans, g_part_cnt);
	for (uint32_t i = 0; i < g_part_cnt; i++)
		part_mans[i]->init();
}

Status Plock::lock(TransactionManager * txn, uint64_t * parts, uint64_t part_cnt) {
	Status rc = OK;
	Time starttime = get_sys_clock();
	uint32_t i;
	for (i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		rc = part_mans[part_id]->lock(txn);
		if (rc == ABORT)
			break;
	}
	if (rc == ABORT) {
		for (uint32_t j = 0; j < i; j++) {
			uint64_t part_id = parts[j];
			part_mans[part_id]->unlock(txn);
		}
		assert(txn->ready_part == 0);
		INC_TMP_STATS(txn->get_thread_id(), time_man, get_sys_clock() - starttime);
		return ABORT;
	}
	if (txn->ready_part > 0) {
		Time t = get_sys_clock();
		while (txn->ready_part > 0) {}
		INC_TMP_STATS(txn->get_thread_id(), time_wait, get_sys_clock() - t);
	}
	assert(txn->ready_part == 0);
	INC_TMP_STATS(txn->get_thread_id(), time_man, get_sys_clock() - starttime);
	return OK;
}

void Plock::unlock(TransactionManager * txn, uint64_t * parts, uint64_t part_cnt) {
	Time starttime = get_sys_clock();
	for (uint32_t i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		part_mans[part_id]->unlock(txn);
	}
	INC_TMP_STATS(txn->get_thread_id(), time_man, get_sys_clock() - starttime);
}
