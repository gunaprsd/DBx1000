#include "Catalog.h"
#include "Row.h"
#include "Table.h"
#include "../system/Allocator.h"
#include "../system/Global.h"
#include "../system/Helper.h"

Table::Table()
{
	this->table_name = NULL;
	this->schema = NULL;
	this->num_rows = 0;
}

Table::~Table() {}

void Table::initialize(Catalog * schema)
{
	this->table_name = schema->table_name;
	this->schema = schema;
}

Status Table::new_row(Row * & row)
{
	assert(false);
	return OK;
}

Status Table::new_row(Row * & row, uint64_t part_id, uint64_t & row_id)
{
	num_rows++;
	row = (Row *) _mm_malloc(sizeof(Row), 64);
	Status status = row->initialize(this, part_id, row_id);
	row->initialize_manager(row);
	return status;
}

Status Table::free_row(Row * & row)
{
	assert(false);
	return OK;
}

uint64_t Table::get_table_size()
{
	return num_rows;
}

Catalog* Table::get_schema()
{
	return schema;
};

const char* 	Table::get_table_name()
{
	return table_name;
};
