#include "Catalog.h"

/*******************************
 * Column Function Definitions
 ******************************/
Column::Column()
{
	this->id = 0;
	this->size = 0;
	this->index = 0;
	this->type = new char[80];
	this->name = new char[80];
}

Column::Column(ColumnId id, uint64_t index, uint64_t size, const char* type, const char* name)
{
	this->id = id;
	this->size = size;
	this->index = index;
	this->type = new char[80];
	this->name = new char[80];
	strcpy(this->type, type);
	strcpy(this->name, name);
};

/********************************
 * Catalog Function Definitions
 ********************************/
void Catalog::initialize(const char * table_name, uint32_t num_columns)
{
	this->table_name = table_name;
	this->num_columns = 0;
	this->tuple_size = 0;
	this->columns = new Column [num_columns];
}

void Catalog::add_column(const char* name, const char* type, uint32_t size)
{
	columns[num_columns].size = size;
	columns[num_columns].id = num_columns;
	columns[num_columns].index = tuple_size;
	strcpy(columns[num_columns].type, type);
	strcpy(columns[num_columns].name, name);
	tuple_size += size;
	num_columns ++;
}

inline uint32_t Catalog::get_tuple_size()
{
	return tuple_size;
};

inline uint32_t Catalog::get_num_columns()
{
	return num_columns;
};

inline uint32_t Catalog::get_column_size(ColumnId id)
{
	return columns[id].size;
};

inline uint32_t Catalog::get_column_index(ColumnId id)
{
	return columns[id].index;
};

inline ColumnId Catalog::get_column_id(const char* name)
{
	uint32_t i;
	for (i = 0; i < num_columns; i++) {
		if (strcmp(name, columns[i].name) == 0)
			break;
	}
	assert (i < num_columns);
	return i;
}

char* Catalog::get_column_type(ColumnId id)
{
	return columns[id].type;
}

char* Catalog::get_column_name(ColumnId id)
{
	return columns[id].name;
}

char* Catalog::get_column_type(const char* name)
{
	return get_column_type( get_column_id(name) );
}

uint32_t Catalog::get_column_index(const char* name)
{
	return get_column_index( get_column_id(name) );
}

void Catalog::print_schema() 
{
	printf("\n[Catalog] %s\n", table_name);
	for (uint32_t i = 0; i < num_columns; i++) {
		printf("\t%s\t%s\t%ld\n", get_column_name(i), get_column_type(i), get_column_size(i));
	}
}
