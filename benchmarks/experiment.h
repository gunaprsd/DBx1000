#ifndef _EXPERIMENT_SYNTH_BM_H_
#define _EXPERIMENT_SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class experiment_query;

class experiment_wl : public Workload {
public :
	Status init();
	Status init_table();
	Status init_schema(string schema_file);
	Status get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
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

class experiment_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, Workload * h_wl, uint64_t part_id); 
	Status run_txn(base_query * query);
private:
	uint64_t row_cnt;
	experiment_wl * _wl;
};

#endif