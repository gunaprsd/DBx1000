#ifndef DBX1000_CC_BFS_SCHEDULER_H
#define DBX1000_CC_BFS_SCHEDULER_H

#include "database.h"
#include "distributions.h"
#include "loader.h"
#include "query.h"
#include "simple_scheduler.h"
#include "thread.h"
#include <atomic>
using namespace std;

/*
 * The scheduling algorithm works as follows:
 * The submit process tries to create a connected component out of the
 * transactions. It submits only the first transaction that is
 * in the connected component.
 *
 */
template <typename T> class CCBFSScheduler : public IOnlineScheduler<T> {
  public:
    CCBFSScheduler(uint64_t num_threads, Database *db) {
        _db = db;
        _num_threads = num_threads;
        _threads = new WorkerThread<T>[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(i, db);
        }
        uint64_t size = AccessIterator<T>::get_max_key();
        data_next_pointer = new uint64_t[size];
        for (uint64_t i = 0; i < size; i++) {
            data_next_pointer[i] = 0;
        }
    }

    void submit_queries() {
        // pthread_t threads[_num_threads];
        ThreadLocalData data[_num_threads];

        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            submit_helper((void *)&data[i]);
            // pthread_create(&threads[i], nullptr, submit_helper, (void *)&data[i]);
        }

        for (uint32_t i = 0; i < _num_threads; i++) {
            // pthread_join(threads[i], nullptr);
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
        auto data = (ThreadLocalData *)ptr;
        auto scheduler = (CCBFSScheduler<T> *)data->fields[0];
        auto thread_id = data->fields[1];

        uint64_t cnt = 0;
        RandomNumberGenerator gen(1);
        gen.seed(0, thread_id + 1);
        QueryIterator<T> *iterator = scheduler->_loader->get_queries_list(thread_id);
        int64_t chosen_core = -1;
        while (!iterator->done()) {
            Query<T> *query = iterator->next();
            ReadWriteSet rwset;
            query->obtain_rw_set(&rwset);
            query->num_links = rwset.num_accesses;
            bool first_in_cc = true;
            for (uint64_t i = 0; i < rwset.num_accesses; i++) {
                bool added = false;
                BaseQuery **outgoing_loc = &(query->links[i].next);
                while (!added) {
                    BaseQuery ***incoming_loc_loc = reinterpret_cast<BaseQuery ***>(
                        &scheduler->data_next_pointer[rwset.accesses[i].key]);
                    BaseQuery **incoming_loc = *incoming_loc_loc;
                    if (__sync_bool_compare_and_swap(incoming_loc_loc, incoming_loc,
                                                     outgoing_loc)) {
                        added = true;
                        cnt++;
                        if (incoming_loc != 0) {
                            *incoming_loc = query;
                            first_in_cc = false;
                        }
                    }
                }
            }

            if (first_in_cc) {
                chosen_core = gen.nextInt64(0) % scheduler->_num_threads;
                scheduler->_threads[chosen_core].submit_query(query);
                printf("Adding a query\n");
            }
        }
        return nullptr;
    }

    uint64_t _num_threads;
    WorkerThread<T> *_threads;
    ParallelWorkloadLoader<T> *_loader;
    uint64_t *data_next_pointer;
    Database *_db;
};

#endif // DBX1000_CC_BFS_SCHEDULER_H
