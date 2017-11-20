#include "vll.h"

#include "Experiment.h"
#include "Expt_Query.h"
#include "YCSB.h"
#include "YCSB_Query.h"
#include "Table.h"
#include "Allocator.h"
#include "TransactionManager.h"
#include "Workload.h"
#include "Row.h"
#include "row_vll.h"
#include "Catalog.h"
#if CC_ALG == VLL

void VLLMan::initialize() 
{
	_txn_queue_size = 0;
	_txn_queue = NULL;
	_txn_queue_tail = NULL;
}

void VLLMan::vllMainLoop(TransactionManager * txn, Query * query)
{
	
	YCSB_Query * m_query = (YCSB_Query *) query;
	// access the indexes. This is not in the critical section
	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		YCSB_Workload * wl = (YCSB_Workload *) txn->get_workload();
		int part_id = wl->key_to_part( req->key );
		INDEX * index = wl->the_index;
		Record * item;
		item = txn->index_read(index, req->key, part_id);
		Row * row = ((Row *)item->location);
		// the following line adds the read/write sets to txn->accesses
		txn->get_row(row, req->rtype);
		//int cs = row->manager->get_cs();
	}

	bool done = false;
	while (!done) {
		TransactionManager * front_txn = NULL;
		uint64_t t5 = get_sys_clock();
		pthread_mutex_lock(&_mutex);
		uint64_t tt5 = get_sys_clock() - t5;
		INC_STATS(txn->get_thread_id(), debug5, tt5);

		TxnQEntry * front = _txn_queue;
		if (front)
			front_txn = front->txn;
		// only one worker thread can execute the txn.
		if (front_txn && front_txn->vll_txn_type == VLL_BLOCKED) {
			front_txn->vll_txn_type = VLL_FREE;
			pthread_mutex_unlock(&_mutex);
			execute(front_txn, query);
			finishTxn( front_txn, front);
		} else {
			// _mutex will be unlocked in beginTxn()
			TxnQEntry * entry = NULL;
			int ok = beginTxn(txn, query, entry);
			if (ok == 2) {
				execute(txn, query);
				finishTxn(txn, entry);
			} 
			assert(ok == 1 || ok == 2);
			done = true;
		}
	}
	return;
}

int VLLMan::beginTxn(TransactionManager * txn, Query * query, TxnQEntry *& entry)
{

	int ret = -1;	
	if (_txn_queue_size >= TXN_QUEUE_SIZE_LIMIT)
		ret = 3;

	txn->vll_txn_type = VLL_FREE;
	assert(WORKLOAD == YCSB);
	
	for (uint32_t rid = 0; rid < txn->row_cnt; rid ++ ) {
		AccessType type = txn->accesses[rid]->type;
		if (txn->accesses[rid]->orig_row->manager->insert_access(type))
			txn->vll_txn_type = VLL_BLOCKED;
	}
	
	entry = getQEntry();
	LIST_PUT_TAIL(_txn_queue, _txn_queue_tail, entry);
	if (txn->vll_txn_type == VLL_BLOCKED)
		ret = 1;
	else 
		ret = 2;
	pthread_mutex_unlock(&_mutex);
	return ret;
}

void VLLMan::execute(TransactionManager * txn, Query * query)
{
	uint64_t t3 = get_sys_clock();
	//YCSB_Query * m_query = (YCSB_Query *) query;
	YCSB_Workload * wl = (YCSB_Workload *) txn->get_workload();
	Catalog * schema = wl->the_table->get_schema();
	uint64_t average;
	for (uint32_t rid = 0; rid < txn->row_cnt; rid ++) {
		Row * row = txn->accesses[rid]->orig_row;
		AccessType type = txn->accesses[rid]->type;
		if (type == RD) {
			for (ColumnId fid = 0; fid < schema->get_num_columns(); fid++) {
				char * data = row->get_data();
				uint64_t fval = *(uint64_t *)(&data[fid * 100]);
				average += fval;
			}
		} else {
			assert(type == WR);
			for (ColumnId fid = 0; fid < schema->get_num_columns(); fid++) {
				char * data = row->get_data();
				*(uint64_t *)(&data[fid * 100]) = 0;
			}
		} 
	}
	uint64_t tt3 = get_sys_clock() - t3;
	INC_STATS(txn->get_thread_id(), debug3, tt3);
}

void VLLMan::finishTxn(TransactionManager * txn, TxnQEntry * entry)
{
	pthread_mutex_lock(&_mutex);
	
	for (uint32_t rid = 0; rid < txn->row_cnt; rid ++ ) {
		AccessType type = txn->accesses[rid]->type;
		txn->accesses[rid]->orig_row->manager->remove_access(type);
	}
	LIST_REMOVE_HT(entry, _txn_queue, _txn_queue_tail);
	pthread_mutex_unlock(&_mutex);
	txn->release();
	mem_allocator.free(txn, 0);
}


TxnQEntry * VLLMan::getQEntry() 
{
	TxnQEntry * entry = (TxnQEntry *) mem_allocator.allocate(sizeof(TxnQEntry), 0);
	entry->prev = NULL;
	entry->next = NULL;
	entry->txn = NULL;
	return entry;
}

void VLLMan::returnQEntry(TxnQEntry * entry) 
{
 	mem_allocator.free(entry, sizeof(TxnQEntry));
}

#endif
