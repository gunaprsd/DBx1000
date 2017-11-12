#include "../storage/BTreeIndex.h"
#include "../storage/HashIndex.h"
#include "../storage/Table.h"
#include "global.h"
#include "helper.h"
#include "experiment.h"
#include "experiment_query.h"
#include "wl.h"
#include "thread.h"
#include "Row.h"
#include "Catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

void experiment_txn_man::init(thread_t * h_thd, Workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (experiment_wl *) h_wl;
}

Status experiment_txn_man::run_txn(base_query * query) {
	Status rc;
	experiment_query * m_query = (experiment_query *) query;
	experiment_wl * wl = (experiment_wl *) h_wl;
	Record * m_item = NULL;
  	row_cnt = 0;

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		experiment_request * req = &m_query->requests[rid];
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
	rc = OK;
final:
	rc = finish(rc);
	return rc;
}

