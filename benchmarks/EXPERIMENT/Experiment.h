#ifndef _EXPERIMENT_SYNTH_BM_H_
#define _EXPERIMENT_SYNTH_BM_H_

#include "../system/Global.h"
#include "../system/Helper.h"
#include "../system/TransactionManager.h"
#include "../system/Workload.h"

class ExperimentQuery;

class experiment_wl : public Workload {
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
		((experiment_wl *)This)->init_table_slice(); 
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class experiment_txn_man : public TransactionManager
{
public:
	void initialize(Thread * h_thd, Workload * h_wl, uint64_t part_id); 
	Status run_transaction(Query * query);
private:
	uint64_t row_cnt;
	experiment_wl * _wl;
};

#endif