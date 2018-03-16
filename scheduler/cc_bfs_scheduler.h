#ifndef DBX1000_CC_BFS_SCHEDULER_H
#define DBX1000_CC_BFS_SCHEDULER_H

#ifdef CONNECTED_COMP_FIELDS
#include "database.h"
#include "distributions.h"
#include "loader.h"
#include "query.h"
#include "simple_scheduler.h"
#include "cc_bfs_thread.h"
#include <atomic>

using namespace std;

/*
 * The scheduling algorithm works as follows:
 * The submit process tries to create a connected component out of the
 * transactions. It submits only the first transaction that is
 * in the connected component.
 */
template <typename T> class CCBFSScheduler : public IOnlineScheduler<T> {
  public:
    CCBFSScheduler(uint64_t num_threads, Database *db) : gen(num_threads) {
        _db = db;
        _num_threads = num_threads;
        _threads = new CCBFSThread<T>[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(i, db, &scheduler_tree);
        }
        uint64_t size = AccessIterator<T>::get_max_key();
        data_next_pointer = new uint64_t[size];
        for (uint64_t i = 0; i < size; i++) {
            data_next_pointer[i] = 0;
        }
    }

    void submit_queries() {
      num_submitted  = 0;
      num_delegated = 0;
	    round_robin_count = 0;
      for (uint64_t thread_id = 0; thread_id < _num_threads; thread_id++) {
            QueryIterator<T> *iterator = _loader->get_queries_list(thread_id);
            while (!iterator->done()) {
                Query<T> *query = iterator->next();
                ReadWriteSet rwset;
                query->obtain_rw_set(&rwset);
                schedule(query, rwset);
            }
        }
        PRINT_INFO(lu, "Num-Submitted", num_submitted);
        PRINT_INFO(lu, "Num-Delegated", num_delegated);
    }

    /*
     * A root is considered invalid if
     * 1. the owner has been set to -1
     * 2. or if the Query<T>* value is nullptr
     */

    void schedule(Query<T> *new_query, const ReadWriteSet &rwset) {
        uint64_t num_active_cc = 0;
        uint64_t min_key = UINT64_MAX;
        uint64_t min_key_index = UINT64_MAX;
        for (uint64_t i = 0; i < rwset.num_accesses; i++) {
            auto key = rwset.accesses[i].key;
            auto current_key_cc = (Query<T> *)data_next_pointer[key];
            root_cc[i] = find_root<T>(current_key_cc);
            if (root_cc[i] != nullptr) {
                num_active_cc++;
                if (key < min_key) {
                    min_key_index = i;
                    min_key = key;
                }
            }
        }

        auto selected_cc = static_cast<Query<T> *>(nullptr);
        if (num_active_cc > 0) {
            selected_cc = root_cc[min_key_index];
            pthread_mutex_lock(&selected_cc->mutex);
            if (selected_cc->owner == -1) {
                // oops! worker marked it done! we need to do this again :(
                pthread_mutex_unlock(&selected_cc->mutex);
                return schedule(new_query, rwset);
            } else {
                if (selected_cc->txn_queue == nullptr) {
                    selected_cc->txn_queue = new queue<Query<T> *>();
                }
                selected_cc->txn_queue->push(new_query);
                num_delegated++;
                assert(selected_cc->owner != -1);
                pthread_mutex_unlock(&selected_cc->mutex);
            }
        } else {
            selected_cc = new_query;
            selected_cc->parent = nullptr;
            selected_cc->owner = 0;
            scheduler_tree.push(selected_cc);
	        round_robin_count++;
            num_submitted++;
        }

        // let our data structures know about our scheduling decision
        for (uint64_t i = 0; i < rwset.num_accesses; i++) {
            auto key = rwset.accesses[i].key;
            auto current_key_cc = (Query<T> *)data_next_pointer[key];
            if (current_key_cc != selected_cc) {
                // update data_next_pointer array so that all future txns
                // that touch this gets sent to this new CC
                while (!ATOM_CAS(data_next_pointer[key], (int64_t)current_key_cc,
                                 (int64_t)selected_cc)) {
                    current_key_cc = (Query<T> *)data_next_pointer[key];
                }

                // update all other CC that txn touches such that
                // root of their CC points to selected CC
                if (current_key_cc != nullptr) {
                    pthread_mutex_lock(&current_key_cc->mutex);
                    auto current_key_root = find_root<T>(current_key_cc);
                    if (current_key_root != selected_cc) {
                        current_key_cc->parent = selected_cc;
                    }
                    pthread_mutex_unlock(&current_key_cc->mutex);
                }
            }
        }
    }

    void run_workers() {
        pthread_t threads[_num_threads];
        ThreadLocalData data[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, execute_helper, (void *)&data[i]);
        }

        for (uint32_t i = 0; i < _num_threads; i++) {
            pthread_join(threads[i], nullptr);
        }
    }

    void schedule(ParallelWorkloadLoader<T> *loader) override {
        _loader = loader;

        uint64_t start_time = get_server_clock();
        submit_queries();
        uint64_t end_time = get_server_clock();
        double duration = DURATION(end_time, start_time);
        printf("Submit Time : %lf secs\n", duration);

        start_time = get_server_clock();
        run_workers();
        end_time = get_server_clock();
        duration = DURATION(end_time, start_time);
        printf("Total Runtime : %lf secs\n", duration);

        // Print the stats
        if (STATS_ENABLE) {
            stats.print();
        }
    }

  protected:
    static void *execute_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto executor = (CCBFSScheduler<T> *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        executor->_threads[thread_id].run();
        return nullptr;
    }

    static void *submit_helper(void *ptr) {
        // Threads must be initialize before
        return nullptr;
    }

    uint64_t _num_threads;
    CCBFSThread<T> *_threads;
    ParallelWorkloadLoader<T> *_loader;
    uint64_t *data_next_pointer;
    Database *_db;

    // Some helpers
    RandomNumberGenerator gen;
    Query<T> *root_cc[MAX_NUM_ACCESSES];
    uint64_t num_submitted;
    uint64_t num_delegated;
	uint64_t round_robin_count;
    tbb::concurrent_queue<Query<T> *> scheduler_tree;
};
#endif

#endif // DBX1000_CC_BFS_SCHEDULER_H
