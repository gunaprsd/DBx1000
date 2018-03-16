
#ifndef DBX1000_THREAD_NEW_H
#define DBX1000_THREAD_NEW_H

#include "loader.h"
#include "database.h"
#include "distributions.h"
#include "manager.h"
#include "query.h"
#include "thread.h"
#include "txn.h"
#include "vll.h"

template <typename T>
class Scheduler {
public:
    Scheduler(uint64_t num_threads, Database *db) {
        _db = db;
        _num_threads = num_threads;
        _threads = new Thread<T>[_num_threads];
        scheduler_tree = new SchedulerTree<T>(static_cast<int32_t>(_num_threads));
        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(i, db, scheduler_tree);
        }
    }
    void submit_queries() {
        int32_t num_threads = static_cast<int32_t>(_num_threads);
        for (int32_t thread_id = 0; thread_id < num_threads; thread_id++) {
            QueryIterator<T> *iterator = _loader->get_queries_list(static_cast<uint64_t>(thread_id));
            while (!iterator->done()) {
                Query<T> *query = iterator->next();
                scheduler_tree->add(query, thread_id);
            }
        }
    }
    void run() {
        pthread_t scheduler_thread;
        ThreadLocalData scheduler_thread_data;
        scheduler_thread_data.fields[0] = (uint64_t)this;
        scheduler_thread_data.fields[1] = (uint64_t)0;
        pthread_create(&scheduler_thread, nullptr, submit_helper, (void *)&scheduler_thread_data);

        //sleep for some time before starting the workers
        usleep(FLAGS_scheduler_delay * 1000);

        pthread_t threads[_num_threads];
        ThreadLocalData data[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, execute_helper, (void *)&data[i]);
        }

        pthread_join(scheduler_thread, nullptr);
        for (uint32_t i = 0; i < _num_threads; i++) {
            pthread_join(threads[i], nullptr);
        }
    }
    void schedule(ParallelWorkloadLoader<T> *loader) {
        _loader = loader;

        uint64_t start_time = get_server_clock();
        submit_queries();
        uint64_t end_time = get_server_clock();
        double duration = DURATION(end_time, start_time);
        printf("Submit Time : %lf secs\n", duration);

        start_time = get_server_clock();
        run();
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
        auto executor = (Scheduler<T> *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        if(FLAGS_abort_buffer) {
            executor->_threads[thread_id].run_with_abort_buffer();
        } else {
            executor->_threads[thread_id].run();
        }
        return nullptr;
    }
    static void *submit_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto executor = (Scheduler<T> *)data->fields[0];
        executor->submit_queries();
        return nullptr;
    }
    uint64_t _num_threads;
    Thread<T> *_threads;
    ParallelWorkloadLoader<T> *_loader;
    ITransactionQueue<T>* scheduler_tree;
    Database *_db;
};

#endif // DBX1000_THREAD_NEW_H
