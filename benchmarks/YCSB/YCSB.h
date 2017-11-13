#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "Global.h"
#include "Helper.h"
#include "TransactionManager.h"
#include "Workload.h"

class YCSB_Query;

class YCSB_Workload : public Workload {
public :
	Status initialize();
	Status initialize_table();
	Status initialize_schema(string schema_file);
	Status get_txn_manager(TransactionManager *& txn_manager, Thread * h_thd);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	Table * the_table;
private:
	void init_table_parallel();
	void * init_table_slice();
	static void * threadInitTable(void * This) {
		((YCSB_Workload *)This)->init_table_slice(); 
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class YCSB_TxnMan : public TransactionManager
{
public:
	void initialize(Thread * h_thd, Workload * h_wl, PartId part_id);
	Status run_transaction(Query * query);
private:
	uint64_t row_cnt;
	YCSB_Workload * _wl;
};

#endif
