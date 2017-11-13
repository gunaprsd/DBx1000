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


void Catalog::print_schema() 
{
	printf("\n[Catalog] %s\n", table_name);
	for (uint32_t i = 0; i < num_columns; i++) {
		printf("\t%s\t%s\t%ud\n", get_column_name(i), get_column_type(i), get_column_size(i));
	}
}
