
#ifndef DBX1000_THREAD_NEW_H
#define DBX1000_THREAD_NEW_H

#include "database.h"
#include "distributions.h"
#include "loader.h"
#include "manager.h"
#include "query.h"
#include "queues.h"
#include "scheduler_tree.h"
#include "scheduler_tree_v1.h"
#include "thread.h"
#include "txn.h"
#include "vll.h"

template<typename T>
class IScheduler {
public:
    virtual void schedule(WorkloadLoader<T> *loader) = 0;
};

template <typename T> class OnlineScheduler : public IScheduler<T> {
  public:
    OnlineScheduler(uint64_t num_threads, Database *db)
        : _num_threads(num_threads), _db(db), _threads(nullptr), _loader(nullptr),
          _txn_queue(nullptr) {
        _threads = new Thread<T>[_num_threads];
        if (FLAGS_scheduler_type.compare("simple_parallel_queues") == 0) {
            _txn_queue = new SimpleParallelQueues<T>(static_cast<int32_t>(_num_threads));
        } else if (FLAGS_scheduler_type.compare("parallel_queues") == 0) {
            _txn_queue = new ParallelQueues<T>(static_cast<int32_t>(_num_threads));
        } else if (FLAGS_scheduler_type.compare("shared_queue") == 0) {
            _txn_queue = new SharedQueue<T>(static_cast<int32_t>(_num_threads));
        } else if (FLAGS_scheduler_type.compare("scheduler_tree") == 0) {
            _txn_queue = new SchedulerTree<T>(static_cast<int32_t>(_num_threads));
        } else if (FLAGS_scheduler_type.compare("scheduler_tree_v1") == 0) {
	        _txn_queue = new SchedulerTreeV1<T>(static_cast<int32_t>(_num_threads));
        } else {
            assert(false);
        }

        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(i, db, _txn_queue);
        }
    }
    void schedule(WorkloadLoader<T> *loader) {
        _loader = loader;
        run_workers();
        // Print the stats
        if (STATS_ENABLE) {
            stats.print();
        }
    }

  protected:
    void submit_queries() {
        int32_t num_threads = static_cast<int32_t>(_num_threads);
        for (int32_t thread_id = 0; thread_id < num_threads; thread_id++) {
            QueryIterator<T> *iterator =
                _loader->get_queries_list(static_cast<uint64_t>(thread_id));
            _txn_queue->load_all(iterator, thread_id);
        }
    }
    void run_workers() {
        uint64_t start_time, end_time;
        if (FLAGS_pre_schedule_txns) {
            printf("Pre-submit transactions\n");
            // submit all queries synchronously
	    start_time = get_server_clock();
	    submit_queries();
	    end_time = get_server_clock();
	    auto duration = DURATION(end_time, start_time);
	    printf("Submission Time: %f\n", duration);

            // notify that all queries are submitted to workers
            for (uint32_t i = 0; i < _num_threads; i++) {
                _threads[i].notify_complete();
            }

            // deploy multiple worker threads
            start_time = get_server_clock();
            pthread_t worker_threads[_num_threads];
            ThreadLocalData data[_num_threads];
            for (uint64_t i = 0; i < _num_threads; i++) {
                data[i].fields[0] = (uint64_t)this;
                data[i].fields[1] = (uint64_t)i;
                pthread_create(&worker_threads[i], nullptr, execute_helper, (void *)&data[i]);
            }

            // join and wait on all worker threads
            for (uint32_t i = 0; i < _num_threads; i++) {
                pthread_join(worker_threads[i], nullptr);
            }
            end_time = get_server_clock();
        } else {
            // create a separate thread for scheduler
            pthread_t scheduler_thread;
            ThreadLocalData scheduler_thread_data;
            scheduler_thread_data.fields[0] = (uint64_t)this;
            scheduler_thread_data.fields[1] = (uint64_t)0;
            pthread_create(&scheduler_thread, nullptr, submit_helper,
                           (void *)&scheduler_thread_data);

            // delay to bootstrap scheduling
            usleep(FLAGS_scheduler_delay * 1000);

            // deploy all worker threads
            start_time = get_server_clock();
            pthread_t worker_threads[_num_threads];
            ThreadLocalData data[_num_threads];
            for (uint64_t i = 0; i < _num_threads; i++) {
                data[i].fields[0] = (uint64_t)this;
                data[i].fields[1] = (uint64_t)i;
                pthread_create(&worker_threads[i], nullptr, execute_helper, (void *)&data[i]);
            }

            // wait until scheduler thread joins
            pthread_join(scheduler_thread, nullptr);

            // notify that all queries are submitted to workers
            for (uint32_t i = 0; i < _num_threads; i++) {
                _threads[i].notify_complete();
            }

            // wait until all workers are done!
            for (uint32_t i = 0; i < _num_threads; i++) {
                pthread_join(worker_threads[i], nullptr);
            }
            end_time = get_server_clock();
        }

        double duration = DURATION(end_time, start_time);
        printf("Total Runtime : %lf secs\n", duration);
    }
    static void *execute_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto executor = (OnlineScheduler<T> *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        if (FLAGS_abort_buffer) {
            executor->_threads[thread_id].run_with_abort_buffer();
        } else {
            executor->_threads[thread_id].run();
        }
        return nullptr;
    }
    static void *submit_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto executor = (OnlineScheduler<T> *)data->fields[0];
        executor->submit_queries();
        return nullptr;
    }

  private:
    uint64_t _num_threads;
    Database *_db;
    Thread<T> *_threads;
    WorkloadLoader<T> *_loader;
    ITransactionQueue<T> *_txn_queue;
};

#endif // DBX1000_THREAD_NEW_H
