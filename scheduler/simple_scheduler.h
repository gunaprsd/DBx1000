
#ifndef DBX1000_THREAD_NEW_H
#define DBX1000_THREAD_NEW_H

#include "database.h"
#include "distributions.h"
#include "manager.h"
#include "query.h"
#include "thread.h"
#include "txn.h"
#include "vll.h"

template <typename T> class IOnlineScheduler {
  public:
    virtual void schedule(ParallelWorkloadLoader<T> *loader) = 0;
};

template <typename T> class SimpleScheduler : public IOnlineScheduler<T> {
  public:
    SimpleScheduler(uint64_t num_threads, Database *db) {
        _db = db;
        _num_threads = num_threads;
        _threads = new WorkerThread<T>[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(i, db);
        }
    }

    void submit_queries() {
        pthread_t threads[_num_threads];
        ThreadLocalData data[_num_threads];

        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, submit_helper, (void *)&data[i]);
        }

        for (uint32_t i = 0; i < _num_threads; i++) {
            pthread_join(threads[i], nullptr);
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
        auto executor = (SimpleScheduler *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        executor->_threads[thread_id].run();
        return nullptr;
    }

    static void *submit_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto scheduler = (SimpleScheduler *)data->fields[0];
        auto thread_id = data->fields[1];

        QueryIterator<T> *iterator = scheduler->_loader->get_queries_list(thread_id);
        while (!iterator->done()) {
            Query<T> *query = iterator->next();
            scheduler->_threads[thread_id].submit_query(query);
        }

        return nullptr;
    }

    uint64_t _num_threads;
    WorkerThread<T> *_threads;
    ParallelWorkloadLoader<T> *_loader;
    Database *_db;
};

#endif // DBX1000_THREAD_NEW_H
