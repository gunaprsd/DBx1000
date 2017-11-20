#include <sched.h>
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "Thread.h"

#include "Expt_Query.h"
#include "Test.h"
#include "TPCC_Query.h"
#include "YCSB_Query.h"
#include "Allocator.h"
#include "Global.h"
#include "Manager.h"
#include "Query.h"
#include "TransactionManager.h"
#include "Workload.h"

void Thread::initialize(ThreadId id, Workload * workload)
{
	this->id = id;
	this->worload = workload;

	srand48_r((id + 1) * get_sys_clock(), &_buffer);
	_abort_buffer_size = ABORT_BUFFER_SIZE;
	_abort_buffer = (AbortBufferEntry *) _mm_malloc(sizeof(AbortBufferEntry) * _abort_buffer_size, 64); 
	for (uint32_t i = 0; i < _abort_buffer_size; i++) {
		_abort_buffer[i].query = NULL;
	}
	_abort_buffer_empty_slots = _abort_buffer_size;
	_abort_buffer_enable = (g_params["abort_buffer_enable"] == "true");
}

Status Thread::run() {
#if !NOGRAPHITE
	id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(id);
	}
	pthread_barrier_wait( & warmup_bar );
	stats.initialize(get_thread_id());
	pthread_barrier_wait( & warmup_bar );

	set_affinity(get_thread_id());

	Random rdm;
	rdm.init(get_thread_id());

	//Obtain and setup Transaction Manager for thread
	Status 					status 		= OK;
	TransactionManager * 	txn_manager 	= NULL;
	status = worload->get_txn_manager(txn_manager, this);
	assert (status == OK);
	glob_manager->set_txn_man(txn_manager);

	Query * 		query 		= NULL;
	uint64_t 	thd_txn_id 	= 0;
	uint64_t 	txn_cnt 		= 0;

	while (true) {
		Time start_ts = get_sys_clock();

		if (WORKLOAD != TEST) {
			int trial = 0;
			if (_abort_buffer_enable) {
				query = NULL;
				while (trial < 2 && query == NULL) {
					Time cur_ts = get_sys_clock();
					Time min_ready_time = UINT64_MAX;

					if (_abort_buffer_empty_slots < _abort_buffer_size) {
						for (uint32_t i = 0; i < _abort_buffer_size; i++) {
							if (_abort_buffer[i].query != NULL && cur_ts > _abort_buffer[i].ready_time) {
								query = _abort_buffer[i].query;
								_abort_buffer[i].query = NULL;
								_abort_buffer[i].ready_time = UINT64_MAX;
								_abort_buffer_empty_slots ++;
								break;
							} else if (_abort_buffer[i].ready_time < min_ready_time) {
								min_ready_time = _abort_buffer[i].ready_time;
							}
						}
					}

					if (query == NULL && _abort_buffer_empty_slots == 0) {
						assert(trial == 0);
						M_ASSERT(min_ready_time >= cur_ts, "min_ready_time=%ld, curr_time=%ld\n", min_ready_time, cur_ts);
						usleep(min_ready_time - cur_ts);
					} else if (query == NULL) {
						query = query_queue->next( id );
					}
				}
			} else {
				if (status == OK) {
					query = query_queue->next( id );
				}
			}
		}

		INC_STATS(id, time_query, get_sys_clock() - start_ts);
		txn_manager->abort_cnt = 0;

		txn_manager->set_transaction_id(get_thread_id() + thd_txn_id * g_thread_cnt);
		thd_txn_id ++;

#if CC_ALG == HSTORE
		txn_manager->set_timestamp(get_next_ts());
		status = part_lock_man.lock(txn_manager, query->part_to_access, query->part_num);
		if(status == OK) {
			status = txn_manager->execute(query);
		}
		part_lock_man.unlock(txn_manager, query->part_to_access, query->part_num);
#elif CC_ALG == VLL
		vll_man.vllMainLoop(txn_manager, query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
		txn_manager->set_timestamp(get_next_ts());
		glob_manager->add_ts(get_thread_id(), txn_manager->get_timestamp());
		if(status == OK) {
			status = txn_manager->execute(query);
		}
#elif CC_ALG == OCC
		txn_manager->start_ts = get_next_ts();
		if(status == OK) {
			status = txn_manager->execute(query);
		}
#else
		if(status == OK) {
			status = txn_manager->execute(query);
		}
#endif

		if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
				|| CC_ALG == MVCC 
				|| CC_ALG == HEKATON
				|| CC_ALG == TIMESTAMP) 
			txn_manager->set_timestamp(get_next_ts());

		status = OK;
#if CC_ALG == HSTORE
		if (WORKLOAD == TEST) {
			uint64_t part_to_access[1] = {0};
			status = part_lock_man.lock(txn_manager, &part_to_access[0], 1);
		} else 
			status = part_lock_man.lock(txn_manager, query->part_to_access, query->part_num);
#elif CC_ALG == VLL
		vll_man.vllMainLoop(txn_manager, query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
		glob_manager->add_ts(get_thread_id(), txn_manager->get_timestamp());
#elif CC_ALG == OCC
		// In the original OCC paper, start_ts only reads the current ts without advancing it.
		// But we advance the global ts here to simplify the implementation. However, the final
		// results should be the same.
		txn_manager->start_ts = get_next_ts(); 
#endif
		if (status == OK) 
		{
#if CC_ALG != VLL
			if (WORKLOAD == TEST)
				status = runTest(txn_manager);
			else 
				status = txn_manager->execute(query);
#endif
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				part_lock_man.unlock(txn_manager, &part_to_access[0], 1);
			} else 
				part_lock_man.unlock(txn_manager, query->part_to_access, query->part_num);
#endif
		}

		//Handle abort of a transaction
		if (status == ABORT) {
			uint64_t penalty = 0;
			if (ABORT_PENALTY != 0)  {
				double r;
				drand48_r(&_buffer, &r);
				penalty = r * ABORT_PENALTY;
			}

			if (!_abort_buffer_enable) {
				usleep(penalty / 1000);
			} else {
				assert(_abort_buffer_empty_slots > 0);
				for (uint32_t i = 0; i < _abort_buffer_size; i ++) {
					if (_abort_buffer[i].query == NULL) {
						_abort_buffer[i].query = query;
						_abort_buffer[i].ready_time = get_sys_clock() + penalty;
						_abort_buffer_empty_slots --;
						break;
					}
				}
			}
		}

		Time end_ts = get_sys_clock();
		uint64_t duration = end_ts - start_ts;

		INC_STATS(get_thread_id(), run_time, duration);
		INC_STATS(get_thread_id(), latency, duration);
		if (status == OK) {
			INC_STATS(get_thread_id(), txn_cnt, 1);
			stats.commit(get_thread_id());
			txn_cnt ++;
		} else if (status == ABORT) {
			INC_STATS(get_thread_id(), time_abort, duration);
			INC_STATS(get_thread_id(), abort_cnt, 1);
			stats.abort(get_thread_id());
			txn_manager->abort_cnt++;
		}

		if (status == FINISH) {
			return status;
		}

		if (! warmup_finish && txn_cnt >= WARMUP / g_thread_cnt) {
			stats.clear( get_thread_id() );
			return FINISH;
		}

		if (warmup_finish && txn_cnt >= MAX_TXN_PER_PART) {
			assert(txn_cnt == MAX_TXN_PER_PART);
	        if( !ATOM_CAS(worload->sim_done, false, true) ) {
				assert( worload->sim_done);
	        }
	    }

	    if (worload->sim_done) {
   		    return FINISH;
   		}
	}
	assert(false);
}

Status Thread::runTest(TransactionManager * txn)
{
	Status status = OK;
	if (g_test_case == READ_WRITE) {
		status = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
		#if CC_ALG == OCC
		txn->start_ts = get_next_ts(); 
		#endif
		status = ((TestTxnMan *)txn)->run_txn(g_test_case, 1);
		printf("READ_WRITE TEST PASSED\n");
		return FINISH;
	} else if (g_test_case == CONFLICT) {
		status = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
		if (status == OK) {
			return FINISH;
		} else {
			return status;
		}
	}
	assert(false);
	return OK;
}
