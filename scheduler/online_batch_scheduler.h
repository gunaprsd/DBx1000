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
    volatile int64_t num_done;

    TaskList(int64_t num_tasks) : tasks(nullptr), num_tasks(num_tasks), num_done(num_tasks) {
        tasks = new Task[num_tasks];
    }
    ~TaskList() { delete[] tasks; }
    bool notify_completion() { return __sync_fetch_and_sub(&num_done, 1) == 1; }
    bool is_done() { return num_done == 0; }
};
struct BatchInfo {
    BatchInfo *prev_batch;
    uint64_t epoch;
    int64_t start_index;
    int64_t end_index;
    int64_t parallelism;
    TaskList *union_tasks;
    TaskList *find_tasks;
    TaskList *execute_tasks;

    BatchInfo(BatchInfo *_prev_batch, uint64_t _epoch, int64_t _start_index, int64_t _end_index,
              int64_t _parallelism)
        : prev_batch(_prev_batch), epoch(_epoch), start_index(_start_index), end_index(_end_index),
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
            INC_STATS(thread_id, time_execute, duration);
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
            INC_STATS(thread_id, time_execute, duration);
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
        : _num_threads(num_threads), _max_batch_size(max_batch_size), _db(db), tasks(), _core_map(),
          _rand(1), done(false) {
        _rand.seed(0, FLAGS_seed + 125);

        // Initialize threads
        _threads = new OnlineBatchExecutor<T>[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            _threads[i].initialize(static_cast<uint32_t>(i), _db);
        }

        // Initialize data items

        auto num_data_items = AccessIterator<T>::get_max_key();
        _data_info = new DataNodeInfo[num_data_items];
        for (uint64_t i = 0; i < num_data_items; i++) {
            _data_info[i].reset(i, 1, 0);
        }

        _epoch = 0;
        _current_batch_index = 0;
        _batch_info = nullptr;
    }
    void schedule(ParallelWorkloadLoader<T> *loader) {
        _loader = loader;
        _loader->get_queries(_batch, _max_size);
        _loader->release();

        prepare();
        move_to_next_batch();
        add_union_tasks();
        execute();

	    if (STATS_ENABLE) {
		    stats.print();
	    }
    }

  protected:
    const uint32_t _num_threads;
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

    void execute() {
        printf("Starting workers...\n");
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
        auto duration = DURATION(end_time, start_time);
        printf("Total Runtime: %lf secs\n", duration);

    }

	static void *execute_helper(void *ptr) {
		auto data = (ThreadLocalData *)ptr;
		auto scheduler = (OnlineBatchScheduler<T> *)data->fields[0];
		auto thread_id = data->fields[1];
		set_affinity(thread_id);
		scheduler->run(thread_id);
		return nullptr;
	}

    void run(uint64_t thread_id) {
        Task *task;

        auto worker_start_time = get_server_clock();

        while (!done) {
            if (!tasks.try_pop(task)) {
                continue;
            }

            switch (task->type) {
            case UNION: {
                auto start_time = get_server_clock();

                // Obtain union info from task and execute it
                auto info = &(task->info.union_info);
                do_union(info->start_index, info->end_index);
                // if you are the last union task, add find tasks
                bool last = task->batch_info->notify_completion(UNION);
                if (last) {
                    add_find_tasks();
                }

                auto end_time = get_server_clock();
                auto duration = (end_time - start_time);
                INC_STATS(thread_id, time_union, duration);
                break;
            }
            case FIND: {
                auto start_time = get_server_clock();

                // Obtain find info from task and execute it
                auto info = &(task->info.find_info);
                do_find(info->start_index, info->end_index);
                // notify completion
                bool last = task->batch_info->notify_completion(FIND);

                auto end_time = get_server_clock();
                auto duration = (end_time - start_time);
                INC_STATS(thread_id, time_find, duration);

                if (last) {
                    if (_batch_info != nullptr) {
                        start_time = get_server_clock();

                        // wait for previous batch to complete
                        if (_batch_info->prev_batch != nullptr) {
                            auto prev_batch = _batch_info->prev_batch;
                            while (!prev_batch->is_done()) { sleep(1); }
                        }

                        end_time = get_server_clock();
                        duration = (end_time - start_time);
                        INC_STATS(thread_id, time_blocked, duration);

                        // now add execute tasks for current batch
                        add_execute_tasks();
                    }

                    // move to the next batch
                    bool success = move_to_next_batch();

                    //if successful, add the union tasks, else you are done
                    if (success) {
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
                assert(false);
            }
        }

        auto worker_end_time = get_server_clock();
        auto worker_duration = (worker_end_time - worker_start_time);
        INC_STATS(thread_id, run_time, worker_duration);
    }

    void prepare() {
        // create data structure for all read write sets
    	_rwset_info = new ReadWriteSet[_max_size];

	    pthread_t worker_threads[_num_threads];
	    ThreadLocalData data[_num_threads];
        auto start_time = get_server_clock();
        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&worker_threads[i], nullptr, prepare_helper, (void *)&data[i]);
        }
        for (uint32_t i = 0; i < _num_threads; i++) {
            pthread_join(worker_threads[i], nullptr);
        }
        auto end_time = get_server_clock();
        auto duration = DURATION(end_time, start_time);
        printf("Preparation Time: %lf secs\n", duration);
    }

	static void *prepare_helper(void *ptr) {
		auto data = (ThreadLocalData *)ptr;
		auto scheduler = (OnlineBatchScheduler<T> *)data->fields[0];
		auto thread_id = data->fields[1];
		set_affinity(thread_id);
		scheduler->compute_read_write_set(thread_id);
		return nullptr;
	}

    void compute_read_write_set(uint64_t thread_id) {
        uint64_t size_per_thread = _max_size / _num_threads;
        uint64_t start_index = thread_id * size_per_thread;
        uint64_t end_index = (thread_id + 1) * size_per_thread;
        end_index = end_index > _max_size ? _max_size : end_index;
        for (auto i = start_index; i < end_index; i++) {
            _batch[i].obtain_rw_set(&(_rwset_info[i]));
        }
    }

  private:
    void add_find_tasks() {
        _core_map.clear();
        int64_t batch_size = (_batch_info->end_index - _batch_info->start_index);
        int64_t batch_size_per_thread = batch_size / _num_threads;
        for (uint64_t i = 0; i < _num_threads; i++) {
            int64_t start_index = i * batch_size_per_thread;
            int64_t end_index = (i + 1) * batch_size_per_thread;
            end_index = (end_index > batch_size) ? batch_size : end_index;
            auto task = &(_batch_info->find_tasks->tasks[i]);
            task->type = FIND;
            task->batch_info = _batch_info;
            task->info.find_info.start_index = _batch_info->start_index + start_index;
            task->info.find_info.end_index = _batch_info->start_index + end_index;
            tasks.push(task);
        }
    }

    void add_execute_tasks() {

        // previous batch is done.

        printf("Adding execute tasks for batch %lu\n", _batch_info->epoch);
        for (uint64_t i = 0; i < _num_threads; i++) {
            auto task = &(_batch_info->execute_tasks->tasks[i]);
            task->type = EXECUTE;
            task->batch_info = _batch_info;
            tasks.push(task);
        }
    }

    void add_union_tasks() {
        //        printf("Adding union tasks for batch %lu\n", _batch_info->epoch);
        int64_t batch_size = (_batch_info->end_index - _batch_info->start_index);
        int64_t batch_size_per_thread = batch_size / _num_threads;
        for (uint64_t i = 0; i < _num_threads; i++) {
            int64_t start_index = i * batch_size_per_thread;
            int64_t end_index = (i + 1) * batch_size_per_thread;
            end_index = (end_index > batch_size) ? batch_size : end_index;
            auto task = &(_batch_info->union_tasks->tasks[i]);
            task->type = UNION;
            task->batch_info = _batch_info;
            task->info.union_info.start_index = _batch_info->start_index + start_index;
            task->info.union_info.end_index = _batch_info->start_index + end_index;
            tasks.push(task);
        }
    }

    bool move_to_next_batch() {
        if (_current_batch_index < _max_size) {
            auto start_index = _current_batch_index;
            _current_batch_index += _max_batch_size;
            // handling corner case
            _current_batch_index =
                (_current_batch_index <= _max_size) ? _current_batch_index : _max_size;
            auto end_index = _current_batch_index;
            __sync_fetch_and_add(&_epoch, 1);
            auto new_batch =
                new BatchInfo(_batch_info, _epoch, start_index, end_index, _num_threads);
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

    long Find(DataNodeInfo *info) {
        EpochAddress old_val;
        EpochAddress new_val;
        old_val.word = info->root_ptr.word;

        // ensure we are in the new epoch!
        if (!old_val.IsEpoch(_epoch)) {
            do {
                EpochAddress self;
                self.Set(info, _epoch);
                __sync_bool_compare_and_swap(&info->root_ptr.word, old_val.word, self.word);
                old_val.word = info->root_ptr.word;
            } while (!old_val.IsEpoch(_epoch));
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
            core = static_cast<long>(_rand.nextInt64(0) % _num_threads);
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
