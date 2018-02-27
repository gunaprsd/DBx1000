#ifndef DBX1000_THREAD_H
#define DBX1000_THREAD_H

#include "database.h"
#include "manager.h"
#include "query.h"
#include "txn.h"
#include "vll.h"
#include "abort_buffer.h"
#include <tbb/concurrent_queue.h>
using namespace tbb;

template <typename T>
using QueryPCQueue = concurrent_queue<Query<T> *>;

template <typename T>
class WorkerThread {
public:
    void initialize(uint32_t id, Database *db) {
        this->thread_id = id;
        this->thread_txn_id = 0;
        this->global_txn_id = thread_txn_id * FLAGS_threads + thread_id;
        this->db = db;
        this->manager = this->db->get_txn_man(thread_id);
        glob_manager->set_txn_man(manager);
        stats.init(thread_id);
    }

    void submit_query(Query<T> *query) {
        input_queue.push(query);
    }

    void ask_to_stop() { this->done = true; }

    void run() {
        auto rc = RCOK;
        auto chosen_query = static_cast<Query<T> *>(nullptr);

        while (!done) {
            if (!abort_buffer.get_ready_query(chosen_query)) {
                if (!input_queue.try_pop(chosen_query)) {
                    // no query in abort buffer, no query in input_queue
                    this->done = true;
                    continue;
                }
            }

            assert(chosen_query != nullptr);

            // prepare manager
            global_txn_id = thread_id + thread_txn_id * FLAGS_threads;
            thread_txn_id++;
            manager->reset(global_txn_id);

            auto start_time = get_sys_clock();
            rc = run_query(chosen_query);
            auto end_time = get_sys_clock();
            auto duration = end_time - start_time;

            // update general statistics
            INC_STATS(thread_id, run_time, duration);
            INC_STATS(thread_id, latency, duration);
            if (rc == RCOK) {
                // update commit statistics
                INC_STATS(thread_id, txn_cnt, 1);
                stats.commit(thread_id);
            } else if (rc == Abort) {
                // add back to abort buffer
                abort_buffer.add_query(chosen_query);

                // update abort statistics
                INC_STATS(thread_id, time_abort, duration);
                INC_STATS(thread_id, abort_cnt, 1);
                stats.abort(thread_id);
            }
        }
    }

    ts_t get_next_ts() {
        if (g_ts_batch_alloc) {
            if (current_timestamp % g_ts_batch_num == 0) {
                current_timestamp = glob_manager->get_ts(thread_id);
                current_timestamp++;
            } else {
                current_timestamp++;
            }
            return current_timestamp - 1;
        } else {
            current_timestamp = glob_manager->get_ts(thread_id);
            return current_timestamp;
        }
    }

protected:
    RC run_query(Query<T> *query) {
        // Prepare transaction manager
        RC rc = RCOK;

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
        rc = manager->run_txn(query);
#elif CC_ALG == OCC
        manager->start_ts = get_next_ts();
        rc = manager->run_txn(query);
#elif CC_ALG == TIMESTAMP
        rc = manager->run_txn(query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
        manager->set_ts(get_next_ts());
        glob_manager->add_ts(thread_id, manager->get_ts());
        rc = manager->run_txn(query);
#elif CC_ALG == HSTORE
        if (!HSTORE_LOCAL_TS) {
            manager->set_ts(get_next_ts());
        }
        if (rc == RCOK) {
            manager->run_txn(query);
        }
#elif CC_ALG == VLL
        vll_man.vllMainLoop(manager, query);
#elif CC_ALG == SILO
        rc = manager->run_txn(query);
#endif

        return rc;
    }

    volatile bool done;
    uint64_t thread_id;
    uint64_t global_txn_id;
    uint64_t thread_txn_id;
    ts_t current_timestamp;
    Database *db;
    QueryPCQueue<T> input_queue;
    TimedAbortBuffer<T> abort_buffer;
    txn_man *manager;
};

#endif // DBX1000_THREAD_H
