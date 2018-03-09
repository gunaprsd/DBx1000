#ifndef DBX1000_CC_BFS_THREAD_H
#define DBX1000_CC_BFS_THREAD_H


#include "abort_buffer.h"
#include "database.h"
#include "manager.h"
#include "query.h"
#include "tbb/concurrent_queue.h"
#include "txn.h"
#include "vll.h"

template <typename T> class CCBFSThread {
  public:
    void initialize(uint32_t id, Database *db) {
        this->thread_id = id;
        this->thread_txn_id = 0;
        this->global_txn_id = thread_txn_id * FLAGS_threads + thread_id;
        this->db = db;
        this->manager = this->db->get_txn_man(thread_id);
        this->done = false;
        glob_manager->set_txn_man(manager);
        stats.init(thread_id);
    }

    void submit_query(Query<T> *query) { input_queue.push(query); }

    void ask_to_stop() { this->done = true; }

    void run() {
        auto rc = RCOK;
        auto chosen_cc = static_cast<Query<T>*>(nullptr);
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        auto move_to_next_cc = true;
        while (!done) {
            if(!abort_buffer.get_ready_query(chosen_query)) {
                // there is no query in abort buffer
                if(move_to_next_cc) {
                    if(input_queue.try_pop(chosen_cc)) {
                        pthread_mutex_lock(&chosen_cc->mutex);
                        chosen_cc->done_with_this = false;
                        chosen_cc->owner = thread_id;
                        move_to_next_cc = false;
                    } else {
		      this->done = true;
		      continue;
                    }
                } else {
                    pthread_mutex_lock(&chosen_cc->mutex);
                }

                assert(chosen_cc != nullptr);
                if(chosen_cc->parent == nullptr) {
                    // still a separate connected component
                    if(!chosen_cc->done_with_this) {
                        chosen_query = chosen_cc;
                        chosen_cc->done_with_this = true;
                    } else {
                        if(chosen_cc->txn_queue == nullptr) {
                            // no additional txns in CC
                            move_to_next_cc = true;
                        } else {
                            // get a txn from queue
                            if(chosen_cc->txn_queue->empty()) {
                                move_to_next_cc = true;
                            } else {
                                chosen_query = chosen_cc->txn_queue->front();
                                chosen_cc->txn_queue->pop();
                            }
                        }
                    }
                } else {
                    // has been merged into a larger CC
                    auto root_cc =find_root<T>(chosen_cc);
                    pthread_mutex_lock(&root_cc->mutex);
                    // check if we can actually delegate!
                    if(root_cc->owner != -1) {
                        if(root_cc->txn_queue == nullptr) {
                            root_cc->txn_queue = new queue<Query<T>*>();
                        }
                        // add own txn
                        if(!chosen_cc->done_with_this) {
                            root_cc->txn_queue->push(chosen_cc);
                            chosen_cc->done_with_this = true;
                        }
                        // add all in the txn_queue
                        if(chosen_cc->txn_queue != nullptr) {
                            while(!chosen_cc->txn_queue->empty()) {
                                root_cc->txn_queue->push(chosen_cc->txn_queue->front());
                                chosen_cc->txn_queue->pop();
                            }
                        }
                        chosen_cc->owner = -1;
                        move_to_next_cc = true;
                    }
                    pthread_mutex_unlock(&root_cc->mutex);
                }
                pthread_mutex_unlock(&chosen_cc->mutex);

                if(move_to_next_cc) {
                    assert(chosen_cc->done_with_this);
                    assert(chosen_cc->txn_queue == nullptr || chosen_cc->txn_queue->empty());
                    chosen_cc = nullptr;
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
    tbb::concurrent_queue<Query<T>*> input_queue;
    TimedAbortBuffer<T> abort_buffer;
    txn_man *manager;
};

#endif //DBX1000_CC_BFS_THREAD_H
