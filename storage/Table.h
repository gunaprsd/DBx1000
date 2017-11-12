#pragma once 

#include "global.h"

#ifndef __STORAGE_TABLE_H__
#define __STORAGE_TABLE_H__

class Catalog;
class Row;

class Table
{
public:
	/* Constructors/Destructors */
					Table			();
					~Table			();

	/* Initializers */
	void 			initialize		(Catalog * schema);


	Status 			new_row			(Row * & row);
	Status 			new_row			(Row * & row, uint64_t part_id, uint64_t &row_id);
	Status 			free_row			(Row * & row);

	/* Getters */
	uint64_t 		get_table_size	();
	Catalog * 		get_schema		();
	const char * 	get_table_name	();

private:
	/* Data Fields */
	Catalog * 		schema;
	uint64_t  		num_rows;
	const char * 	table_name;
	char 			padding[CACHE_LINE_SIZE - sizeof(Catalog*) + sizeof(uint64_t) + sizeof(char *)];
};

#endif
