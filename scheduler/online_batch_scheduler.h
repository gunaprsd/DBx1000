#ifndef DBX1000_ONLINE_BATCH_SCHEDULER_H
#define DBX1000_ONLINE_BATCH_SCHEDULER_H

#include "abort_buffer.h"
#include "database.h"
#include "global.h"
#include "loader.h"
#include "manager.h"
#include "partitioner.h"
#include "query.h"
#include "txn.h"
#include "vll.h"

#include <tbb/concurrent_queue.h>

struct Task;
struct BatchInfo;
enum SchedulerTaskType { UNION, FIND, EXECUTE };
struct UnionTaskInfo {
    int64_t start_index;
    int64_t end_index;
};
struct FindTaskInfo {
    int64_t start_index;
    int64_t end_index;
};
struct ExecuteTaskInfo {
    tbb::concurrent_queue<BaseQuery *> *queries;
};
union SchedulerTaskInfo {
    UnionTaskInfo union_info;
    FindTaskInfo find_info;
    ExecuteTaskInfo execute_info;
};
struct Task {
    BatchInfo *batch_info;
    SchedulerTaskType type;
    SchedulerTaskInfo info;
};
struct TaskList {
    Task *tasks;
    int64_t num_tasks;
    int64_t num_done;

    TaskList(int64_t num_tasks) : tasks(nullptr), num_tasks(num_tasks), num_done(0) {
        tasks = new Task[num_tasks];
    }
    ~TaskList() { delete[] tasks; }
    bool notify_completion() { return __sync_add_and_fetch(&num_done, 1) == num_tasks; }
    bool is_done() { return num_done == num_tasks; }
};
struct BatchInfo {
    BatchInfo *prev_batch;
    int64_t start_index;
    int64_t end_index;
    int64_t parallelism;
    TaskList *union_tasks;
    TaskList *find_tasks;
    TaskList *execute_tasks;

    BatchInfo(BatchInfo *_prev_batch, int64_t _start_index, int64_t _end_index,
              int64_t _parallelism)
        : prev_batch(_prev_batch), start_index(_start_index), end_index(_end_index),
          parallelism(_parallelism) {
        union_tasks = new TaskList(parallelism);
        find_tasks = new TaskList(parallelism);
        execute_tasks = new TaskList(parallelism);
        for (int64_t i = 0; i < parallelism; i++) {
            execute_tasks->tasks[i].info.execute_info.queries =
                new tbb::concurrent_queue<BaseQuery *>();
        }
    }
    ~BatchInfo() {
        for (int64_t i = 0; i < parallelism; i++) {
            delete execute_tasks->tasks[i].info.execute_info.queries;
        }
        delete union_tasks;
        delete find_tasks;
        delete execute_tasks;
    }
    bool is_done() { return execute_tasks->is_done(); }
    bool notify_completion(SchedulerTaskType type) {
        switch (type) {
        case UNION:
            return union_tasks->notify_completion();
        case FIND:
            return find_tasks->notify_completion();
        case EXECUTE:
            return execute_tasks->notify_completion();
        default:
            return false;
        }
    }
};
template <typename T> class OnlineBatchExecutor {
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
    void run(tbb::concurrent_queue<BaseQuery *> &queries) {
        auto rc = RCOK;
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        auto done = false;
        // int32_t tid = static_cast<int32_t>(thread_id);
        while (!done) {
            // get next query
            BaseQuery *tquery;
            if (!queries.try_pop(tquery)) {
                done = true;
                continue;
            } else {
                chosen_query = static_cast<Query<T> *>(tquery);
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
                INC_STATS(thread_id, txn_cnt, 1);
                stats.commit(thread_id);
            } else if (rc == Abort) {
                INC_STATS(thread_id, time_abort, duration);
                INC_STATS(thread_id, abort_cnt, 1);
                stats.abort(thread_id);
            }
        }
    }
    void run_with_abort_buffer(tbb::concurrent_queue<BaseQuery *> &queries) {
        RC rc;
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        auto done = false;
        while (!done) {
            if (!abort_buffer.get_ready_query(chosen_query)) {
                BaseQuery *tquery;
                if (!queries.try_pop(tquery)) {
                    // no query in abort buffer, no query in transaction_queue
                    if (abort_buffer.empty()) {
                        done = true;
                    }
                    continue;
                } else {
                    chosen_query = (Query<T> *)tquery;
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
template <typename T> class OnlineBatchScheduler {
  public:
    OnlineBatchScheduler(uint32_t num_threads, uint32_t max_batch_size, Database *db)
        : _num_threads(num_threads),
          _num_clusters(num_threads),
          _max_batch_size(max_batch_size),
          _db(db), tasks(), _core_map(), _rand(1), done(false) {
        _rand.seed(0, FLAGS_seed + 125);

        // Initialize threads
        _threads = new OnlineBatchExecutor<T>[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(static_cast<uint32_t>(i), _db);
        }

        // Initialize data items
	    _epoch = 0;
	    auto num_data_items = AccessIterator<T>::get_max_key();
	    _data_info = new DataNodeInfo[num_data_items];
	    for (uint64_t i = 0; i < num_data_items; i++) {
		    _data_info[i].reset(i, 0, 0);
	    }
    }

    void schedule(ParallelWorkloadLoader<T> *loader) {
        printf("Preparing...\n");
        _loader = loader;
        _loader->get_queries(_batch, _max_size);
        _loader->release();

        // Prepare read-write set
        _rwset_info = new ReadWriteSet[_max_size];
        for (uint64_t i = 0; i < _max_size; i++) {
            _batch[i].obtain_rw_set(&(_rwset_info[i]));
        }

        move_to_next_batch();
        add_union_tasks();

        printf("Starting workers...");
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

  protected:
    const uint32_t _num_threads;
    const uint32_t _num_clusters;
    const uint64_t _max_batch_size;
	Database *_db;
	tbb::concurrent_queue<Task *> tasks;
	tbb::concurrent_unordered_map<long, long> _core_map;
	RandomNumberGenerator _rand;
	volatile bool done;
    OnlineBatchExecutor<T> *_threads;
    ParallelWorkloadLoader<T> *_loader;
    DataNodeInfo *_data_info;

    // current batch info
	uint64_t _epoch;
    uint64_t _current_batch_index;
	BatchInfo *_batch_info;

    // Input batch of all transactions
    uint64_t _max_size;
	Query<T> *_batch;
	ReadWriteSet *_rwset_info;

    void run_worker(uint64_t thread_id) {
        // must collocate share work between union, find and execute
        Task *task;

        while (!done) {
            if (!tasks.try_pop(task)) {
                continue;
            }

            switch (task->type) {
            case UNION: {
                auto info = &(task->info.union_info);
                do_union(info->start_index, info->end_index);
                bool last = task->batch_info->notify_completion(UNION);
                if (last) {
                    add_find_tasks();
                }
                break;
            }
            case FIND: {
                auto info = &(task->info.find_info);
                do_find(info->start_index, info->end_index);
                bool last = task->batch_info->notify_completion(FIND);
                if (last) {
                    if (_batch_info != nullptr) {
                        add_execute_tasks();
                    }

                    bool next = move_to_next_batch();
                    if(next) {
                        add_union_tasks();
                    } else {
                        done = true;
                    }
                }
                break;
            }
            case EXECUTE: {
                auto info = &(task->info.execute_info);
                _threads[thread_id].run(*info->queries);
                task->batch_info->notify_completion(EXECUTE);
                break;
            }
            default:
                break;
            }
        }
    }
    void add_find_tasks() {
        // add find task for current batch
        _core_map.clear();
        int64_t batch_size = _batch_info->end_index - _batch_info->start_index;
        int64_t batch_size_per_thread = batch_size / _num_threads;
        for (uint64_t i = 0; i < _num_threads; i++) {
            int64_t start_index = i * batch_size_per_thread;
            int64_t end_index = (i + 1) * batch_size_per_thread;
            end_index = end_index > batch_size ? batch_size : end_index;
            auto task = &(_batch_info->find_tasks->tasks[i]);
            task->type = FIND;
            task->batch_info = _batch_info;
            task->info.find_info.start_index = start_index;
            task->info.find_info.end_index = end_index;
            tasks.push(task);
        }
    }
    void add_execute_tasks() {
    	if(_batch_info->prev_batch != nullptr) {
    		auto prev_batch = _batch_info->prev_batch;
    		while(!prev_batch->is_done());
    	}
    	// previous batch is done.

        for (uint64_t i = 0; i < _num_threads; i++) {
            auto task = &(_batch_info->find_tasks->tasks[i]);
            task->type = EXECUTE;
            task->batch_info = _batch_info;
            tasks.push(task);
        }
    }
    void add_union_tasks() {
        int64_t batch_size = _batch_info->end_index - _batch_info->start_index;
        int64_t batch_size_per_thread = batch_size / _num_threads;
        for (uint64_t i = 0; i < _num_threads; i++) {
            int64_t start_index = i * batch_size_per_thread;
            int64_t end_index = (i + 1) * batch_size_per_thread;
            end_index = end_index > batch_size ? batch_size : end_index;
            auto task = &(_batch_info->union_tasks->tasks[i]);
            task->type = UNION;
            task->batch_info = _batch_info;
            task->info.union_info.start_index = start_index;
            task->info.union_info.end_index = end_index;
            tasks.push(task);
        }
    }
    bool move_to_next_batch() {
        if(_current_batch_index < _max_batch_size) {
            auto start_index = _current_batch_index;
            _current_batch_index += _max_batch_size;
            _current_batch_index =
                    (_current_batch_index >= _max_size) ? _max_size : _current_batch_index;
            auto end_index = _current_batch_index;
            _epoch++;
            auto new_batch = new BatchInfo(_batch_info, start_index, end_index, _num_threads);
            _batch_info = new_batch;
            return true;
        } else {
            return false;
        }
    }
    void do_union(int64_t start, int64_t end) {
        for (int64_t t = start; t < end; t++) {
            ReadWriteSet *rwset = &(_rwset_info[t]);
            for (uint32_t i = 0; i < rwset->num_accesses; i++) {
                auto key1 = rwset->accesses[i].key;
                auto data_info1 = &(_data_info[key1]);
                for (uint32_t j = i + 1; j < rwset->num_accesses; j++) {
                    auto key2 = rwset->accesses[j].key;
                    auto data_info2 = &(_data_info[key2]);
                    Union(data_info1, data_info2);
                }
            }
        }
    }
    void do_find(int64_t start, int64_t end) {
        unordered_map<long, long> local_core_map;
        for (int64_t t = start; t < end; t++) {
            auto rwset = &(_rwset_info[t]);
            auto key = rwset->accesses[0].key;
            auto data_info = &(_data_info[key]);
            auto cc = Find(data_info);
            long core = -1;
            auto iter = local_core_map.find(cc);
            if (iter == local_core_map.end()) {
                core = GetOrAllocateCore(cc);
                local_core_map[cc] = core;
            } else {
                core = iter->second;
            }
            auto task = &(_batch_info->execute_tasks->tasks[core]);
            task->info.execute_info.queries->push(&_batch[t]);
        }
    }
    static void *execute_helper(void *ptr) {
        auto data = (ThreadLocalData *)ptr;
        auto scheduler = (OnlineBatchScheduler<T> *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        scheduler->run_worker(thread_id);
        return nullptr;
    }

  private:
    long Find(DataNodeInfo *info) {
        EpochAddress old_val;
        EpochAddress new_val;
        old_val.word = info->root_ptr.word;

        // ensure we are in the new epoch!
        if(!old_val.IsEpoch(_epoch)) {
            do {
                EpochAddress self;
                self.Set(info, _epoch);
                __sync_bool_compare_and_swap(&info->root_ptr.word, old_val.word, self.word);
                old_val.word = info->root_ptr.word;
            } while(!old_val.IsEpoch(_epoch));
        }

        // find with path compression
        auto current_root = static_cast<DataNodeInfo *>(old_val.GetAddress());
        if (current_root != info) {
            new_val.word = Find(current_root);
            if (old_val.word != new_val.word) {
                __sync_bool_compare_and_swap(&(info->root_ptr.word), old_val.word, new_val.word);
            }
        }

        // return the word at root_ptr
        return info->root_ptr.word;
    }
    void Union(DataNodeInfo *p, DataNodeInfo *q) {
        EpochAddress info1, info2;
        info1.word = Find(p);
        info2.word = Find(q);
        if (info1.word == info2.word) {
            return;
        }

        auto root1 = static_cast<DataNodeInfo *>(info1.GetAddress());
        auto root2 = static_cast<DataNodeInfo *>(info2.GetAddress());

        if (root1->size < root2->size) {
            if (__sync_bool_compare_and_swap(&root1->root_ptr.word, info1.word, info2.word)) {
                __sync_fetch_and_add(&root2->size, root1->size);
                return;
            }
        } else {
            if (__sync_bool_compare_and_swap(&root2->root_ptr.word, info2.word, info1.word)) {
                __sync_fetch_and_add(&root1->size, root2->size);
                return;
            }
        }

        Union(p, q);
    }
    long GetOrAllocateCore(long word) {
        long core = -1;
        auto iter = _core_map.find(word);
        if (iter == _core_map.end()) {
            core = static_cast<long>(_rand.nextInt64(0) % _num_clusters);
            auto res = _core_map.insert(std::pair<long, long>(word, core));
            if (res.second) {
                return core;
            } else {
                return GetOrAllocateCore(word);
            }
        } else {
            core = iter->second;
        }
        return core;
    }
};

#endif // DBX1000_ONLINE_BATCH_SCHEDULER_H
