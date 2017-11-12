#pragma once 

#ifndef __STORAGE_ABSTRACT_INDEX_H__
#define __STORAGE_ABSTRACT_INDEX_H__

#include "global.h"
#include "helper.h"

class Table;

class AbstractIndex
{
public:
	/* Constructors, Destructors*/
							AbstractIndex	() { table = NULL; }
	virtual 					~AbstractIndex	() {}

	/* Initialize Functions */
	virtual Status 			initialize	() { return OK; };
	virtual Status 			initialize	(uint64_t size) { return OK; };

	/* Index Accessor Functions */
	virtual bool 			exists	(KeyId key_id) = 0;
	virtual Status 			insert	(KeyId key_id, Record * item, PartId part_id = -1) = 0;
	virtual Status	 		read		(KeyId key_id, Record * & item, PartId part_id = -1) = 0;
	virtual Status	 		read		(KeyId key_id, Record * & item, PartId part_id = -1, ThreadId thd_id = 0) = 0;
	virtual Status 			remove	(KeyId key) { return OK; };

	/* Data Fields */
	Table * 					table;
};

#endif
