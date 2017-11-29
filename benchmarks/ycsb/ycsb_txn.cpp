#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "graph_partitioner.h"
#include <vector>
#include "mem_alloc.h"

void YCSBTransactionManager::initialize(Database * database, uint32_t thread_id) {
	txn_man::initialize(database, thread_id);
	db = (YCSBDatabase *) database;
}

RC YCSBTransactionManager::run_txn(BaseQuery * query) {
	RC rc;
	ycsb_params * m_query = & ((ycsb_query *) query)->params;
	YCSBDatabase * wl = (YCSBDatabase *) database;
	itemid_t * m_item = NULL;
  	row_cnt = 0;

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
		while ( !finish_req ) {
			if (iteration == 0) {
				m_item = index_read(db->the_index, req->key, part_id);
			} 
#if INDEX_STRUCT == IDX_BTREE
			else {
				the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			row_t * row = ((row_t *)m_item->location);
			row_t * row_local; 
			access_t type = req->rtype;
			
			row_local = get_row(row, type);
			if (row_local == NULL) {
				rc = Abort;
				goto final;
			}

			// Computation //
			// Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row_local->get_data();
						__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                  }
                } else {
                    assert(req->rtype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row->get_data();
						*(uint64_t *)(&data[fid * 10]) = 0;
//					}
                } 
            }


			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}
	rc = RCOK;
final:
	rc = finish(rc);
	return rc;
}


void YCSBExecutor::initialize(uint32_t num_threads) {
	BenchmarkExecutor::initialize(num_threads);

	//Build database in parallel
	_db = new YCSBDatabase();
	_db->initialize(INIT_PARALLELISM);
	_db->load();

	//Generate workload in parallel
	_generator = new YCSBWorkloadGenerator();
	_generator->initialize(_num_threads, MAX_TXN_PER_PART * _num_threads, NULL);

	//Initialize each thread
	for(uint32_t i = 0; i < _num_threads; i++) {
		_threads[i].initialize(i, _db, _generator->get_queries_list(i), MAX_TXN_PER_PART, true);
	}
}
