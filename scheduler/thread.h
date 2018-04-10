#ifndef DBX1000_THREAD_H
#define DBX1000_THREAD_H

#include "abort_buffer.h"
#include "database.h"
#include "manager.h"
#include "query.h"
#include "txn.h"
#include "vll.h"

template <typename T> class Thread {
  public:
    void initialize(uint32_t id, Database *db, ITransactionQueue<T> *scheduler_tree) {
        this->thread_id = id;
        this->thread_txn_id = 0;
        this->global_txn_id = thread_txn_id * FLAGS_threads + thread_id;
        this->db = db;
        this->manager = this->db->get_txn_man(thread_id);
        this->submitted_all_queries = false;
        this->scheduler_tree = scheduler_tree;
        glob_manager->set_txn_man(manager);
        stats.init(thread_id);
    }
    void notify_complete() { this->submitted_all_queries = true; }
    void run() {
        auto begin_time = get_server_clock();
        auto rc = RCOK;
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        auto done = false;
        int32_t tid = static_cast<int32_t>(thread_id);
        while (!done) {
            // get next query
            if (!scheduler_tree->next(tid, chosen_query)) {
                if (submitted_all_queries) {
                    done = true;
                }
                continue;
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
            INC_STATS(thread_id, time_execute, duration);
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
        auto final_time = get_server_clock();
        auto runtime_duration = (final_time - begin_time);
        INC_STATS(thread_id, run_time, runtime_duration);
    }
    void run_with_abort_buffer() {
        RC rc;
        auto begin_time = get_server_clock();
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        auto done = false;
        while (!done) {
            if (!abort_buffer.get_ready_query(chosen_query)) {
                if (!scheduler_tree->next(static_cast<int32_t>(thread_id), chosen_query)) {
                    // no query in abort buffer, no query in transaction_queue
                    if (submitted_all_queries) {
                        done = true;
                    }
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
            INC_STATS(thread_id, time_execute, duration);
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
        auto final_time = get_server_clock();
        auto runtime_duration = (final_time - begin_time);
        INC_STATS(thread_id, run_time, runtime_duration);
    }

  protected:
    RC run_query(Query<T> *query) {
        // Prepare transaction manager
        RC rc = RCOK;

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT || CC_ALG == NONE
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

  private:
    uint64_t thread_id;
    uint64_t global_txn_id;
    uint64_t thread_txn_id;
    ts_t current_timestamp;
    Database *db;
    ITransactionQueue<T> *scheduler_tree;
    TimedAbortBuffer<T> abort_buffer;
    txn_man *manager;
    volatile bool submitted_all_queries;
};

#endif // DBX1000_THREAD_H
