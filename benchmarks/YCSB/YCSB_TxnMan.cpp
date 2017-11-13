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
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "YCSB.h"
#include "YCSB_Query.h"

void YCSB_TxnMan::initialize(Thread * h_thd, Workload * h_wl, uint64_t thd_id) {
	TransactionManager::initialize(h_thd, h_wl, thd_id);
	_wl = (YCSB_Workload *) h_wl;
}

Status YCSB_TxnMan::run_transaction(Query * query) {
	Status rc;
	YCSB_Query * m_query = (YCSB_Query *) query;
	YCSB_Workload * wl = (YCSB_Workload *) workload;
	Record * m_item = NULL;
  	row_cnt = 0;

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
		while ( !finish_req ) {
			if (iteration == 0) {
				m_item = index_read(_wl->the_index, req->key, part_id);
			} 
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			Row * row = ((Row *)m_item->location);
			Row * row_local; 
			AccessType type = req->rtype;
			
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
	rc = OK;
final:
	rc = finish(rc);
	return rc;
}

