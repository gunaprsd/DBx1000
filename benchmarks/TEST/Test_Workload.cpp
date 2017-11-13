#include "BTreeIndex.h"
#include "HashIndex.h"
#include "Table.h"
#include "Allocator.h"
#include "Thread.h"
#include "Row.h"
#include "Test.h"

Status TestWorkload::initialize() {
	Workload::initialize();
	string path;
	path = "./benchmarks/TEST/schema.txt";
	init_schema( path.c_str() );

	initialize_table();
	return OK;
}

Status TestWorkload::init_schema(const char * schema_file) {
	Workload::initialize_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return OK;
}

Status TestWorkload::initialize_table() {
	Status rc = OK;
	for (int rid = 0; rid < 10; rid ++) {
		Row * new_row = NULL;
		uint64_t row_id;
		int part_id = 0;
        rc = the_table->new_row(new_row, part_id, row_id); 
		assert(rc == OK);
		uint64_t primary_key = rid;
		new_row->set_primary_key(primary_key);
        new_row->set_value(0, rid);
        new_row->set_value(1, 0);
        new_row->set_value(2, 0);
        Record * m_item = (Record *) mem_allocator.allocate( sizeof(Record), part_id );
		assert(m_item != NULL);
		m_item->type = DT_ROW;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
        rc = the_index->insert(idx_key, m_item, 0);
        assert(rc == OK);
    }
	return rc;
}

Status TestWorkload::get_txn_manager(TransactionManager *& txn_manager, Thread * h_thd) {
	txn_manager = (TestTxnMan *)
		mem_allocator.allocate( sizeof(TestTxnMan), h_thd->get_thd_id() );
	new(txn_manager) TestTxnMan();
	txn_manager->initialize(h_thd, this, h_thd->get_thd_id());
	return OK;
}

void TestWorkload::summarize() {
	uint64_t curr_time = get_sys_clock();
	if (g_test_case == CONFLICT) {
		assert(curr_time - time > g_thread_cnt * 1e9);
		int total_wait_cnt = 0;
		for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
			total_wait_cnt += stats._stats[tid]->wait_cnt;
		}
		printf("CONFLICT TEST. PASSED.\n");
	}
}
