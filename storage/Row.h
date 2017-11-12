#pragma once 

#include <cassert>
#include "global.h"
#include "Catalog.h"
#include "Table.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void set_value(int col_id, type value) { set_value(col_id, &value); }

#define DECL_GET_VALUE(type)\
	void get_value(int col_id, type & value);

#define GET_VALUE(type)\
	void Row::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
		value = *(type *)&data[pos];\
	}

#define GET_POS(col_id) get_schema()->get_field_index(col_id)
#define GET_SIZE(col_id)get_schema()->get_field_size(col_id)
#define GET_ID(col_name) get_schema()->get_field_id(col_name)

class Table;
class txn_man;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_vll;

class Row
{
public:

	Status 			initialize(Table * host_table, uint64_t part_id, uint64_t row_id = 0);
	void 			initialize(int size);
	void 			initialize_manager(Row * row);
	Status 			switch_schema(Table * host_table);

	/* Table Properties */
	Table * 			get_table() { return table; }
	char * 			get_data() { return data; }
	Catalog * 		get_schema() { return get_table()->get_schema(); }
	const char * 	get_table_name() { return get_table()->get_table_name(); };
	uint64_t 		get_tuple_size() { return get_schema()->get_tuple_size(); }
	uint64_t 		get_field_cnt() { return get_schema()->field_cnt; }

	/* Private Fields */
	uint64_t 		get_row_id() { return _row_id; };
	uint64_t 		get_part_id() { return _part_id; };
	uint64_t 		get_primary_key() {return _primary_key; };
	void 			set_primary_key(uint64_t key) { _primary_key = key; };


	/* Crude Data Modifiers */
	void				set_data(char* data, uint64_t offset, uint64_t size) { memcpy(&(this->data[offset]), data, size); }
	void 			copy(Row * src) { set_data(src->get_data(), src->get_tuple_size()); }
	void 			set_data(char * data, uint64_t size) { set_data(data, 0, size); }
	void 			set_value(int id, void * ptr) { set_data((char*)ptr, GET_POS(id), GET_SIZE(id));  }
	void 			set_value(int id, void * ptr, int size) { set_data((char*)ptr, GET_POS(id), size); }
	void 			set_value(const char * col_name, void * ptr) { set_value( GET_ID(col_name), ptr); }
	char * 			get_value(int id) { return &data[GET_POS(id)]; }
	char * 			get_value(char * col_name) { return &data[ GET_POS( GET_ID(col_name) ) ]; }
	

	/* Typed Data Modifiers */
	void 			set_value(int col_id, uint64_t value) { set_value(col_id, & value); }
	void 			set_value(int col_id, int64_t value) { set_value(col_id, & value); }
	void 			set_value(int col_id, uint32_t value) { set_value(col_id, & value); }
	void 			set_value(int col_id, int32_t value) { set_value(col_id, & value); }
	void 			set_value(int col_id, double value) { set_value(col_id, & value); }

	void 			get_value(int col_id, uint64_t & value) { value = *(uint64_t*) & data[GET_POS(col_id)]; }
	void 			get_value(int col_id, int64_t & value) { value = *(int64_t*) & data[GET_POS(col_id)]; }
	void 			get_value(int col_id, uint32_t & value) { value = *(uint32_t*) & data[GET_POS(col_id)]; }
	void 			get_value(int col_id, int32_t & value) { value = *(int32_t*) & data[GET_POS(col_id)]; }
	void 			get_value(int col_id, double & value) { value = *(double*) & data[GET_POS(col_id)]; }

	void 			free_row() { free(data); }


	// for concurrency control. can be lock, timestamp etc.
	Status 			get_row(access_t type, txn_man * txn, Row *& row);
	void 			return_row(access_t type, txn_man * txn, Row * row);
	
  #if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    Row_lock * manager;
  #elif CC_ALG == TIMESTAMP
   	Row_ts * manager;
  #elif CC_ALG == MVCC
  	Row_mvcc * manager;
  #elif CC_ALG == HEKATON
  	Row_hekaton * manager;
  #elif CC_ALG == OCC
  	Row_occ * manager;
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
	uint64_t 		_primary_key;
	uint64_t			_part_id;
	uint64_t 		_row_id;
};
