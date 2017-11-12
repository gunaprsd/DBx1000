#pragma once 

#include "global.h"

class Row;
class Table;
class HashIndex;
class BTreeIndex;
class Catalog;
class lock_man;
class txn_man;
class thread_t;
class AbstractIndex;
class Timestamp;
class Mvcc;

// this is the base class for all workload
class Workload
{
public:
	// tables indexed by table name
	map<string, Table *> tables;
	map<string, INDEX *> indexes;
	
	// initialize the tables and indexes.
	virtual Status init();
	virtual Status init_schema(string schema_file);
	virtual Status init_table()=0;
	virtual Status get_txn_man(txn_man *& txn_manager, thread_t * h_thd)=0;
	
	bool sim_done;
protected:
	void index_insert(string index_name, uint64_t key, Row * row);
	void index_insert(INDEX * index, uint64_t key, Row * row, int64_t part_id = -1);
};

