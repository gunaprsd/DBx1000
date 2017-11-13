#ifndef _TEST_H_
#define _TEST_H_

#include "Global.h"
#include "TransactionManager.h"
#include "Workload.h"

class TestWorkload : public Workload
{
public:
	Status initialize();
	Status initialize_table();
	Status init_schema(const char * schema_file);
	Status get_txn_manager(TransactionManager *& txn_manager, Thread * h_thd);
	void summarize();
	void tick() { time = get_sys_clock(); };
	INDEX * the_index;
	Table * the_table;
private:
	uint64_t time;
};

class TestTxnMan : public TransactionManager 
{
public:
	void initialize(Thread * h_thd, Workload * h_wl, uint64_t part_id); 
	Status run_txn(int type, int access_num);
	Status run_transaction(Query * m_query) { assert(false); };
private:
	Status testReadwrite(int access_num);
	Status testConflict(int access_num);
	
	TestWorkload * _wl;
};

#endif
