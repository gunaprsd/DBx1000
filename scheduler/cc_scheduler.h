#ifndef DBX1000_CC_SCHEDULER_H
#define DBX1000_CC_SCHEDULER_H

#include "database.h"
#include "loader.h"
#include "distributions.h"
#include "query.h"
#include "thread.h"
#include "simple_scheduler.h"

template <typename T>
class CCScheduler : public IOnlineScheduler<T> {
  public:
	CCScheduler(uint64_t num_threads) {
        _num_threads = num_threads;
        _threads = (WorkerThread<T> *)_mm_malloc(sizeof(WorkerThread<T>) * _num_threads, 64);
		uint64_t size = AccessIterator<T>::get_max_key();
		data_next_pointer = new uint64_t[size];
		for (uint64_t i = 0; i < size; i++) {
			data_next_pointer[i] = 0;
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

    void schedule(ParallelWorkloadLoader<T>* loader) override {
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
        auto scheduler = (CCScheduler<T> *)data->fields[0];
        auto thread_id = data->fields[1];

	    uint64_t cnt = 0;
        QueryIterator<T>* iterator = scheduler->_loader->get_queries_list(thread_id);
	    int64_t chosen_core = -1;
	    while(!iterator->done()) {
		    Query<T>* query = iterator->next();
		    ReadWriteSet rwset;
		    query->obtain_rw_set(&rwset);
			query->num_links = rwset.num_accesses;

		    for (uint64_t i = 0; i < rwset.num_accesses; i++) {
			    bool added = false;
			    BaseQuery **outgoing_loc = &(query->links[i].next);
			    BaseQuery **incoming_loc =
					    reinterpret_cast<BaseQuery **>(scheduler->data_next_pointer[rwset.accesses[i].key]);
			    if (incoming_loc != 0) {
				    chosen_core = (*incoming_loc)->core;
				    break;
			    }
		    }

		    if(chosen_core == -1) {
			    ((atomic<int64_t>*)& (query->core))->store(chosen_core);
		    }

		    for (uint64_t i = 0; i < rwset.num_accesses; i++) {
			    bool added = false;
			    BaseQuery **outgoing_loc = &(query->links[i].next);
			    BaseQuery **incoming_loc =
					    reinterpret_cast<BaseQuery **>(scheduler->data_next_pointer[rwset.accesses[i].key]);
			    while (!added) {
				    BaseQuery ***incoming_loc_loc = &incoming_loc;
				    if (__sync_bool_compare_and_swap(incoming_loc_loc, incoming_loc,
				                                     outgoing_loc)) {
					    added = true;
					    cnt++;
					    if (incoming_loc != 0) {
						    *incoming_loc = query;
					    }
				    }
			    }
		    }

		    scheduler->_threads[chosen_core].submit_query(query);
	    }
        return nullptr;
    }

    uint64_t _num_threads;
    WorkerThread<T> *_threads;
	ParallelWorkloadLoader<T>* _loader;
	uint64_t* data_next_pointer;
};


#endif //DBX1000_CC_SCHEDULER_H
