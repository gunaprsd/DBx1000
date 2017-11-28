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
#include "graph_creator.h"
#include <vector>
#include "mem_alloc.h"

void YCSBTransactionManager::initialize(Database * database, INDEX * index, uint32_t thread_id) {
	txn_man::initialize(database, thread_id);
	ycsb_database = (YCSBDatabase *) database;
	the_index = index;
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
				m_item = index_read(the_index, req->key, part_id);
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
	_db->initialize();
	_db->load();

	//Generate workload in parallel
	_generator = new YCSBWorkloadGenerator();
	_generator->initialize(_num_threads, MAX_TXN_PER_PART * _num_threads, NULL);

	//Initialize each thread
	for(uint32_t i = 0; i < _num_threads; i++) {
		_threads[i].initialize(i, _db, _generator->get_queries_list(i), MAX_TXN_PER_PART, true);
	}
}

void YCSBGraphGenerator::create_conflict_graph(uint64_t start_offset, uint64_t num_records, FILE *out_file) {
    //Step 1: read all the queries into an array
    ycsb_query * all_queries = new ycsb_query[num_records * _num_threads];
    for(uint32_t thread_id = 0; thread_id < _num_threads; thread_id++) {
        ycsb_query * ptr = all_queries + (thread_id * num_records);
        size_t read_bytes = fread(ptr, sizeof(ycsb_query), num_records, _files[thread_id]);
		assert(read_bytes == num_records);
    }

    //Step 2: create a DataNode map for items in the transaction
    uint64_t num_total_queries = num_records * _num_threads;
    uint64_t hash_size = 16 * num_total_queries * MAX_REQ_PER_QUERY;
    DataInfo * data_info = new DataInfo[hash_size];
    for(uint64_t i = 0; i < num_total_queries; i++) {
        ycsb_query * query = & all_queries[i];
        for(uint32_t j = 0; j < query->params.request_cnt; j++) {
            uint64_t  key_hash = hash(query->params.requests[j].key);
            key_hash = key_hash % hash_size;
            if(query->params.requests[j].rtype == RD) {
                data_info[key_hash].num_reads++;
            } else {
                data_info[key_hash].num_writes++;
            }
        }
    }

    //Step 3: write out transactions file
    for(uint32_t i = 0; i < 10; i++) {
        for(uint32_t j = 0; j < 20; j++) {
            fprintf(out_file, "%%");
        }
        fprintf(out_file, "\n");
    }


    uint64_t  num_edges = 0;
    for(uint64_t i = 0; i < num_total_queries; i++) {
        fprintf(out_file, "%ld", all_queries[i].params.request_cnt);
        for(uint64_t j = 0; j < num_total_queries; j++) {
            double weight = compute_weight(& all_queries[i], & all_queries[j], data_info);
            if(weight < 0) {
                continue;
            } else {
	      fprintf(out_file, " %ld %ld", j+1, (long)weight);
                num_edges++;
            }
        }
        fprintf(out_file, "\n");
    }

    fseek(out_file, 0, SEEK_SET);
    fprintf(out_file, "%ld %ld %s\n", num_total_queries, num_edges/2, "011");
}
