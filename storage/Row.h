#pragma once 
#ifndef __STORAGE_ROW_H__
#define __STORAGE_ROW_H__

#include <cassert>
#include "Global.h"
#include "Catalog.h"
#include "Table.h"



#define GET_POS(col_id) get_schema()->get_column_index(col_id)
#define GET_SIZE(col_id) get_schema()->get_column_size(col_id)
#define GET_ID(col_name) get_schema()->get_column_id(col_name)

class TransactionManager;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class RowOcc;
class Row_tictoc;
class Row_silo;
class Row_vll;

class Row
{
public:

	Status 			initialize			(Table * host_table, uint64_t part_id, uint64_t row_id = 0);
	void 			initialize			(int size);
	void 			initialize_manager	(Row * row);
	Status 			switch_schema		(Table * host_table);

	/* Table Properties */
	Table * 			get_table		();
	char * 			get_data			();
	Catalog * 		get_schema		();
	const char * 	get_table_name	();
	uint64_t 		get_tuple_size	();
	uint64_t 		get_field_cnt	();

	/* Private Fields */
	PartId 			get_part_id		();
	RowId 			get_row_id		();
	Key 				get_primary_key	();
	void 			set_primary_key	(Key key);


	/* Crude Data Modifiers */
	void				set_data		(char * data, uint64_t offset, uint64_t size);
	void 			set_data		(char * data, uint64_t size);

	void 			set_value	(ColumnId id, void * ptr);
	void 			set_value	(ColumnId id, void * ptr, int size);
	void 			set_value	(const char * col_name, void * ptr);
	char * 			get_value	(ColumnId id);
	char * 			get_value	(const char * col_name);
	void 			copy			(Row * src);

	/* Typed Data Modifiers */
	void 			set_value	(ColumnId col_id, uint64_t value);
	void 			set_value	(ColumnId col_id, uint32_t value);
	void 			set_value	(ColumnId col_id, int64_t value);
	void 			set_value	(ColumnId col_id, int32_t value);
	void 			set_value	(ColumnId col_id, double value);

	void 			get_value	(ColumnId col_id, uint64_t & value);
	void 			get_value	(ColumnId col_id, int64_t & value);
	void 			get_value	(ColumnId col_id, uint32_t & value);
	void 			get_value	(ColumnId col_id, int32_t & value);
	void 			get_value	(ColumnId col_id, double & value);

	void 			free_row		();


	Status 			request_access		(AccessType type, TransactionManager * txn, Row *& row);
	void 			return_access		(AccessType type, TransactionManager * txn, Row * row);
	
  #if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    Row_lock * manager;
  #elif CC_ALG == TIMESTAMP
   	Row_ts * manager;
  #elif CC_ALG == MVCC
  	Row_mvcc * manager;
  #elif CC_ALG == HEKATON
  	Row_hekaton * manager;
  #elif CC_ALG == OCC
  	RowOcc * manager;
  #elif CC_ALG == TICTOC
  	Row_tictoc * manager;
  #elif CC_ALG == SILO
  	Row_silo * manager;
  #elif CC_ALG == VLL
  	Row_vll * manager;
  #endif

  	char * data;
	Table * table;
private:
	// primary key should be calculated from the data stored in the row.
	Key		 		_primary_key;
	PartId			_part_id;
	RowId	 		_row_id;
};

/***********************************
 * INLINE FUNCTION DEFINITIONS
 **********************************/

inline Table * Row::get_table()
{
	return table;
}

inline char * Row::get_data()
{
	return data;
}

inline Catalog * Row::get_schema()
{
	return get_table()->get_schema();
}

inline const char * Row::get_table_name()
{
	return get_table()->get_table_name();
}

inline uint64_t Row::get_tuple_size()
{
	return get_schema()->get_tuple_size();
}

inline uint64_t Row::get_field_cnt()
{
	return get_schema()->num_columns;
}


inline RowId Row::get_row_id()
{
	return _row_id;
}

inline PartId Row::get_part_id()
{
	return _part_id;
}

inline Key Row::get_primary_key()
{
	return _primary_key;
}

inline void Row::set_primary_key(Key key)
{
	_primary_key = key;
}



inline void	Row::set_data(char * data, uint64_t offset, uint64_t size)
{
	memcpy(& (this->data[offset]), data, size);
}


inline void 	Row::set_data(char * data, uint64_t size)
{
	set_data(data, 0, size);
}

inline void 	Row::set_value(ColumnId id, void * ptr)
{
	set_data((char*)ptr, GET_POS(id), GET_SIZE(id));
}

inline void 	Row::set_value(ColumnId id, void * ptr, int size)
{
	set_data((char*)ptr, GET_POS(id), size);
}

inline void 	Row::set_value(const char * col_name, void * ptr)
{
	set_value( GET_ID(col_name), ptr);
}


inline void 	Row::set_value(ColumnId col_id, uint64_t value)
{
	*(uint64_t*)(& data[GET_POS(col_id)]) = value;
}

inline void 	Row::set_value(ColumnId col_id, int64_t value)
{
	*(int64_t*)(& data[GET_POS(col_id)]) = value;
}

inline void 	Row::set_value(ColumnId col_id, uint32_t value)
{
	*(uint32_t*)(& data[GET_POS(col_id)]) = value;
}

inline void 	Row::set_value(ColumnId col_id, int32_t value)
{
	*(int32_t*)(& data[GET_POS(col_id)]) = value;
}

inline void 	Row::set_value(ColumnId col_id, double value)
{
	*(double*)(& data[GET_POS(col_id)]) = value;
}


inline char * Row::get_value(ColumnId id)
{
	return & data[GET_POS(id)];
}

inline char * Row::get_value(const char * col_name)
{
	return & data[ GET_POS( GET_ID(col_name) ) ];
}

inline void 	Row::get_value(ColumnId col_id, uint64_t & value)
{
	value = *(uint64_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, int64_t & value)
{
	value = *(int64_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, uint32_t & value)
{
	value = *(uint32_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, int32_t & value)
{
	value = *(int32_t*) & data[GET_POS(col_id)];
}

inline void 	Row::get_value(ColumnId col_id, double & value)
{
	value = *(double*) & data[GET_POS(col_id)];
}

inline void 	Row::free_row()
{
	free(data);
}

inline Status Row::initialize(Table * host_table, uint64_t part_id, uint64_t row_id)
{
	table = host_table;
	_row_id = row_id;
	_part_id = part_id;
	Catalog * schema = host_table->get_schema();
	int tuple_size = schema->get_tuple_size();
	data = (char *) _mm_malloc(sizeof(char) * tuple_size, 64);
	return OK;
}

inline void Row::initialize(int size)
{
	data = (char *) _mm_malloc(size, 64);
}

#endif
