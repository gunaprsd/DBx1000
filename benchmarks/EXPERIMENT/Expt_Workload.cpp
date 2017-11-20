#include <sched.h>

#include "BTreeIndex.h"
#include "HashIndex.h"
#include "Table.h"
#include "Allocator.h"
#include "Global.h"
#include "Helper.h"
#include "Manager.h"
#include "Query.h"
#include "Thread.h"
#include "Workload.h"
#include "Row.h"
#include "Catalog.h"
#include "Experiment.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"

int experiment_wl::next_tid;

Status experiment_wl::initialize() 
{
	Workload::initialize();
	next_tid = 0;
	char * cpath = getenv("GRAPHITE_HOME");
	string path;
	if (cpath == NULL) 
		path = "./benchmarks/EXPERIMENT/schema.txt";
	else { 
		path = string(cpath);
		path += "/tests/apps/dbms/EXPERIMENT_schema.txt";
	}
	initialize_schema( path );
	
	init_table_parallel();
	//init_table();
	return OK;
}

Status experiment_wl::initialize_schema(string schema_file) 
{
	Workload::initialize_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return OK;
}
	
int experiment_wl::key_to_part(uint64_t key) 
{
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

Status experiment_wl::initialize_table() 
{
	Status rc;
    uint64_t total_row = 0;
    while (true) {
    	for (uint32_t part_id = 0; part_id < g_part_cnt; part_id ++) {
            if (total_row > g_synth_table_size)
                goto ins_done;
            Row * new_row = NULL;
			uint64_t row_id;
            rc = the_table->new_row(new_row, part_id, row_id); 
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == OK);
			uint64_t primary_key = total_row;
			new_row->set_primary_key(primary_key);
            new_row->set_value(0, primary_key);
			Catalog * schema = the_table->get_schema();
			for (uint32_t fid = 0; fid < schema->get_num_columns(); fid ++) {
				int field_size = schema->get_column_size(fid);
				char value[field_size];
				for (int i = 0; i < field_size; i++) 
					value[i] = (char)rand() % (1<<8) ;
				new_row->set_value(fid, value);
			}
            Record * m_item = 
                (Record *) mem_allocator.allocate( sizeof(Record), part_id );
			assert(m_item != NULL);
            m_item->type = DT_ROW;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->insert(idx_key, m_item, part_id);
            assert(rc == OK);
            total_row ++;
        }
    }
	ins_done:
    	printf("[EXPERIMENT] Table \"MAIN_TABLE\" initialized.\n");
    	return OK;

}

// init table in parallel
void experiment_wl::init_table_parallel() 
{
	enable_thread_mem_pool = true;
	pthread_t p_thds[g_init_parallelism - 1];
	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
	mem_allocator.unregister();
}

void * experiment_wl::init_table_slice() 
{
	uint32_t tid = ATOM_FETCH_ADD(next_tid, 1);
	// set cpu affinity
	set_affinity(tid);

	mem_allocator.register_thread(tid);
	Status rc;
	assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	while ((uint32_t)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
	assert((uint32_t)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	for (uint64_t key = slice_size * tid; 
			key < slice_size * (tid + 1); 
			key ++
	) {
		Row * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key);
		rc = the_table->new_row(new_row, part_id, row_id); 
		assert(rc == OK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, primary_key);
		Catalog * schema = the_table->get_schema();
		
		for (uint32_t fid = 0; fid < schema->get_num_columns(); fid ++) {
			char value[6] = "hello";
			new_row->set_value(fid, value);
		}

		Record * m_item =
			(Record *) mem_allocator.allocate( sizeof(Record), part_id );
		assert(m_item != NULL);
		m_item->type = DT_ROW;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
		
		rc = the_index->insert(idx_key, m_item, part_id);
		assert(rc == OK);
	}
	return NULL;
}

Status experiment_wl::get_txn_manager(TransactionManager *& txn_manager, Thread * h_thd)
{
	txn_manager = (experiment_txn_man *)
		_mm_malloc( sizeof(experiment_txn_man), 64 );
	new(txn_manager) experiment_txn_man();
	txn_manager->initialize(h_thd, this, h_thd->get_thread_id());
	return OK;
}
