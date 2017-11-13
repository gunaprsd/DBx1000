#pragma once 

#include <map>
#include <vector>
#include "global.h"
#include "helper.h"

#ifndef __STORAGE_CATALOG_H__
#define __STORAGE_CATALOG_H__

typedef uint32_t ColumnId;

struct Column
{
	Column	();
	Column	(ColumnId id, uint64_t index, uint64_t size, const char* type, const char* name);

	/* Data Fields */
	ColumnId 	id;
	uint64_t 	size;
	uint64_t 	index;
	char* 		type;
	char* 		name;
	char 		padding[CACHE_LINE_SIZE - sizeof(ColumnId) - sizeof(uint64_t) * 2 - sizeof(char*) * 2];
};

struct Catalog
{
	void 			initialize	(const char * table_name, uint32_t num_columns);
	void 			add_column	(const char * name, const char * type, uint32_t size);

	uint32_t 		get_tuple_size		();
	uint32_t 		get_num_columns		();
	ColumnId 		get_column_id		(const char * name);
	uint32_t 		get_column_index		(const char * name);
	char * 			get_column_type		(const char * name);
	uint32_t 		get_column_size		(ColumnId id);
	uint32_t 		get_column_index		(ColumnId id);
	char * 			get_column_name		(ColumnId id);
	char * 			get_column_type		(ColumnId id);
	void 			print_schema			();

	/* Data Fields */
	uint32_t 		num_columns;
	uint32_t 		tuple_size;
	Column * 		columns;
	char * 			table_name;
};

#endif
