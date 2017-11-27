#include <sched.h>
#include "ycsb.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"



void YCSBDatabase::initialize(uint32_t num_threads) {
	Database::initialize(num_threads);
	char * cpath = getenv("GRAPHITE_HOME");
	string path;
	if (cpath == NULL) 
		path = "./benchmarks/YCSB_schema.txt";
	else { 
		path = string(cpath);
		path += "/tests/apps/dbms/YCSB_schema.txt";
	}
	Database::initialize_schema(path);
	the_table = tables["MAIN_TABLE"];
	the_index = indexes["MAIN_INDEX"];
}
	
int YCSBDatabase::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

txn_man *  YCSBDatabase::get_txn_man(uint32_t thread_id) {
	auto txn_manager = (YCSBTransactionManager *) _mm_malloc(sizeof(YCSBTransactionManager), 64);
	txn_manager->initialize(this, the_index, thread_id);
	return txn_manager;
}

void YCSBDatabase::load_tables(uint32_t thread_id) {
	mem_allocator.register_thread(thread_id);
	load_main_table(thread_id);
	mem_allocator.unregister();
}

void YCSBDatabase::load_main_table(uint32_t tid) {
	RC rc;
	uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	for (uint64_t key = slice_size * tid; key < slice_size * (tid + 1); key++) {
		row_t * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key);
		rc = the_table->get_new_row(new_row, part_id, row_id); 
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);
		Catalog * schema = the_table->get_schema();
		
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
			char value[6] = "hello";
			new_row->set_value(fid, value);
		}

		itemid_t * m_item =
			(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
		
		rc = the_index->index_insert(idx_key, m_item, part_id);
		assert(rc == RCOK);
	}
}
