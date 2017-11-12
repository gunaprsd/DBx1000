#ifndef _TEST_H_
#define _TEST_H_

#include "global.h"
#include "txn.h"
#include "wl.h"

class TestWorkload : public Workload
{
public:
	Status init();
	Status init_table();
	Status init_schema(const char * schema_file);
	Status get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	void summarize();
	void tick() { time = get_sys_clock(); };
	INDEX * the_index;
	Table * the_table;
private:
	uint64_t time;
};

class TestTxnMan : public txn_man 
{
public:
	void init(thread_t * h_thd, Workload * h_wl, uint64_t part_id); 
	Status run_txn(int type, int access_num);
	Status run_txn(base_query * m_query) { assert(false); };
private:
	Status testReadwrite(int access_num);
	Status testConflict(int access_num);
	
	TestWorkload * _wl;
};

#endif
