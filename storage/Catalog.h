#pragma once 

#include <map>
#include <vector>
#include "global.h"
#include "helper.h"

class Column 
{
public:
	Column() 
	{
		this->type = new char[80];
		this->name = new char[80];
	}

	Column(uint64_t size, char* type, char* name, uint64_t id, uint64_t index) 
	{
		this->size = size;
		this->id = id;
		this->index = index;
		this->type = new char[80];
		this->name = new char[80];
		strcpy(this->type, type);
		strcpy(this->name, name);
	};

	uint64_t id;
	uint64_t size;
	uint64_t index;
	char* type;
	char* name;
	char padding[CACHE_LINE_SIZE - sizeof(uint64_t) * 3 - sizeof(char*) * 2];
};

class Catalog 
{
public:
	void initialize(const char * table_name, int field_cnt);
	void add_column(char * col_name, uint64_t size, char * type);

	uint32_t 			field_cnt;
 	const char * 	table_name;
	
	uint32_t 		get_tuple_size() { return tuple_size; };
	uint64_t 		get_field_cnt() { return field_cnt; };
	uint64_t 		get_field_size(int id) { return columns[id].size; };
	uint64_t 		get_field_index(int id) { return columns[id].index; };
	char * 			get_field_type(uint64_t id);
	char * 			get_field_name(uint64_t id);
	uint64_t 		get_field_id(const char * name);
	char * 			get_field_type(char * name);
	uint64_t 		get_field_index(char * name);

	void 			print_schema();
	Column * 		columns;
	UInt32 			tuple_size;
};

