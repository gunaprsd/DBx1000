#ifndef DBX1000_ONLINE_BATCH_SCHEDULER_H
#define DBX1000_ONLINE_BATCH_SCHEDULER_H

#include "global.h"
#include "database.h"
#include "loader.h"
#include "partitioner.h"
#include "online_batch_thread.h"
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

/*
 * Online Batch Scheduler clusters a batch of transactions
 * into smaller clusters that can be executed on its own
 */
template<typename T>
class OnlineBatchScheduler {
	tbb::concurrent_queue<SchedulerTaskInfo*> tasks;
	tbb::concurrent_unordered_map<DataNodeInfo*, SchedulerTaskInfo*> core_map;
	uint64_t _num_threads;
	Database *_db;
	OnlineBatchThread<T> *_threads;
	ParallelWorkloadLoader<T> *_loader;


};

#endif // DBX1000_ONLINE_BATCH_SCHEDULER_H
