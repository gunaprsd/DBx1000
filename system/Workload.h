#pragma once 
#ifndef __SYSTEM_WORKLOAD_H__
#define __SYSTEM_WORKLOAD_H__

#include "Global.h"

class Row;
class Table;
class HashIndex;
class BTreeIndex;
class Catalog;
class lock_man;
class TransactionManager;
class Thread;
class AbstractIndex;
class Timestamp;
class Mvcc;

class Workload
{
public:
	/* Constructors/Destructors */
				Workload();
	virtual		~Workload();

	/* Initializers */
	virtual Status	 	initialize			();
	virtual Status 		initialize_schema	(string schema_file);
	virtual Status 		initialize_table	() = 0;

	virtual Status 		get_txn_manager		(TransactionManager * & txn_manager, Thread * h_thd)=0;
	
	/* Data Fields */
	bool 					sim_done;
	map<string, Table *> 	tables;
	map<string, INDEX *> 	indexes;


protected:
	void 	insert_into_index	(string index_name, Key key, Row * row);
	void 	insert_into_index	(INDEX * index, Key key, Row * row, PartId part_id = -1);
};

#endif