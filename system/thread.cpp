#include <sched.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "database.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "mem_alloc.h"
#include "thread.h"
#include "thread_queue.h"

void Thread::initialize(uint32_t id,
                        Database * db,
                        BaseQueryList * query_list,
                        bool abort_buffer_enable) {
    this->thread_id = id;
    this->db = db;
    this->query_queue = new ThreadQueue();
    query_queue->initialize(id, query_list, ABORT_BUFFER_ENABLE);
}

RC Thread::run() {
    stats.init(thread_id);
    set_affinity(thread_id);

    RC rc = RCOK;
    txn_man * m_txn = db->get_txn_man(thread_id);
    glob_manager->set_txn_man(m_txn);

    BaseQuery * m_query = nullptr;
    uint64_t thd_txn_id = 0;

    for(uint32_t run_id = 0; run_id < g_repeat; run_id++) {
        query_queue->reset();
        while (!query_queue->done()) {
            ts_t start_time = get_sys_clock();

            //Step 1: Obtain a query from the query queue
            m_query = query_queue->next_query();
            INC_STATS(thread_id, time_query, get_sys_clock() - start_time);

            //Step 2: Prepare the transaction manager
            uint64_t global_txn_id = thread_id + thd_txn_id * g_thread_cnt;
            thd_txn_id++;
            m_txn->reset(global_txn_id);

            //Step 3: Execute the transaction
            rc = RCOK;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
            rc = m_txn->run_txn(m_query);
#elif CC_ALG == OCC
            m_txn->start_ts = get_next_ts();
        rc = m_txn->run_txn(m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
				m_txn->set_ts(get_next_ts());
				glob_manager->add_ts(thread_id, m_txn->get_ts());
				rc = m_txn->run_txn(m_query);
#elif CC_ALG == HSTORE
			if(!HSTORE_LOCAL_TS) {
				m_txn->set_ts(get_next_ts());
			}
			//rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
			if(rc == RCOK) {
				m_txn->run_txn(m_query);
			}
			//part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
			vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == SILO
			rc = m_txn->run_txn(m_query);
#endif

            //Step 4: Return status to query queue
            query_queue->return_status(rc);

            ts_t end_time = get_sys_clock();

            //Step 5: Update statistics
            uint64_t duration = end_time - start_time;
            INC_STATS(thread_id, run_time, duration);
            INC_STATS(thread_id, latency, duration);
            if (rc == RCOK) {
                INC_STATS(thread_id, txn_cnt, 1);
                stats.commit(thread_id);
            } else if (rc == Abort) {
                INC_STATS(thread_id, time_abort, duration);
                INC_STATS(thread_id, abort_cnt, 1);
                stats.abort(thread_id);
            }
        }
    }


    return FINISH;
}

ts_t Thread::get_next_ts() {
    if (g_ts_batch_alloc) {
        if (_curr_ts % g_ts_batch_num == 0) {
            _curr_ts = glob_manager->get_ts(thread_id);
            _curr_ts ++;
        } else {
            _curr_ts ++;
        }
        return _curr_ts - 1;
    } else {
        _curr_ts = glob_manager->get_ts(thread_id);
        return _curr_ts;
    }
}



void BenchmarkExecutor::execute() {
    pthread_t threads[_num_threads];
    ThreadLocalData data[_num_threads];

    uint64_t start_time = get_server_clock();
    for(uint32_t i = 0; i < _num_threads; i++) {
        data[i].fields[0] = (uint64_t)this;
        data[i].fields[1] = (uint64_t)i;
        pthread_create(&threads[i], nullptr, execute_helper, (void *)&data[i]);
    }

    for(uint32_t i = 0; i < _num_threads; i++) {
        pthread_join(threads[i], nullptr);
    }

    uint64_t end_time = get_server_clock();
    double duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
    printf("Total Runtime : %lf secs\n", duration);
    if (STATS_ENABLE) {
        stats.print();
    }
}

void * BenchmarkExecutor::execute_helper(void * ptr) {
    //Threads must be initialize before
    auto data = (ThreadLocalData *) ptr;
    auto executor = (BenchmarkExecutor *) data->fields[0];
    auto thread_id = (uint32_t) data->fields[1];

    executor->_threads[thread_id].run();
    return nullptr;
}

void BenchmarkExecutor::release() {

}

void BenchmarkExecutor::initialize(uint32_t num_threads, const char *path) {
    _num_threads = num_threads;
    _threads = (Thread *) _mm_malloc(sizeof(Thread) * _num_threads, 64);
    strcpy(_path, path);
}
