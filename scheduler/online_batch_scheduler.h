#ifndef DBX1000_ONLINE_BATCH_SCHEDULER_H
#define DBX1000_ONLINE_BATCH_SCHEDULER_H

#include "global.h"
#include "database.h"
#include "loader.h"
#include "partitioner.h"
#include "abort_buffer.h"
#include "manager.h"
#include "query.h"
#include "txn.h"
#include "vll.h"

#include <tbb/concurrent_queue.h>

enum SchedulerTaskType { UNION, FIND, EXECUTE };
struct UnionTaskInfo {};
struct FindTaskInfo {};
struct ExecuteTaskInfo {};
union SchedulerTaskInfo {
    UnionTaskInfo union_info;
    FindTaskInfo find_info;
    ExecuteTaskInfo execute_info;
};
struct Task {
	SchedulerTaskType type;
    SchedulerTaskInfo info;
};


template<typename T>
class OnlineBatchExecutor {
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
	void run(ExecuteTaskInfo* task) {

	}
	void run_with_abort_buffer(ExecuteTaskInfo* task) {

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
	TimedAbortBuffer<T> abort_buffer;
	txn_man *manager;
};


/*
 * Online Batch Scheduler clusters a batch of transactions
 * into smaller clusters that can be executed on its own
 */
template<typename T>
class OnlineBatchScheduler {
public:
	OnlineBatchScheduler(uint64_t num_threads, Database *db) {
		_num_threads = num_threads;
		_db = db;
		_threads = new OnlineBatchExecutor<T>[_num_threads];
		for(uint64_t i = 0; i < _num_threads; i++) {
			_threads[i].initialize(i, _db);
		}
	}
	void schedule(ParallelWorkloadLoader<T> *loader) {
		_loader = loader;
		auto start_time = get_server_clock();
		pthread_t worker_threads[_num_threads];
		ThreadLocalData data[_num_threads];
		for (uint64_t i = 0; i < _num_threads; i++) {
			data[i].fields[0] = (uint64_t)this;
			data[i].fields[1] = (uint64_t)i;
			pthread_create(&worker_threads[i], nullptr, execute_helper, (void *)&data[i]);
		}

		// wait until all workers are done!
		for (uint32_t i = 0; i < _num_threads; i++) {
			pthread_join(worker_threads[i], nullptr);
		}
		auto end_time = get_server_clock();
		double duration = DURATION(end_time, start_time);
		printf("Total Runtime : %lf secs\n", duration);
	}

	tbb::concurrent_queue<SchedulerTaskInfo*> tasks;
	tbb::concurrent_unordered_map<DataNodeInfo*, SchedulerTaskInfo*> core_map;
	uint64_t _num_threads;
	Database *_db;
	OnlineBatchExecutor<T> *_threads;
	ParallelWorkloadLoader<T> *_loader;

protected:
	void run_worker(uint64_t thread_id) {
		// must collocate share work between union, find and execute
	}
	static void* execute_helper(void* ptr) {
		auto data = (ThreadLocalData *)ptr;
		auto scheduler = (OnlineBatchScheduler<T> *)data->fields[0];
		auto thread_id = data->fields[1];
		set_affinity(thread_id);
		scheduler->run_worker(thread_id);
		return nullptr;
	}
};

#endif // DBX1000_ONLINE_BATCH_SCHEDULER_H
