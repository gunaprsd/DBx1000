#ifndef DBX1000_ONLINE_BATCH_SCHEDULER_V2_H
#define DBX1000_ONLINE_BATCH_SCHEDULER_V2_H

#include "abort_buffer.h"
#include "partitioner_helper.h"
#include "scheduler.h"
#include <tbb/concurrent_unordered_map.h>
#include <unordered_map>
using namespace std;

template <typename K, typename V> using ConcurrentHashMap = tbb::concurrent_unordered_map<K, V>;

template <typename T> using ConcurrentQueue = tbb::concurrent_queue<T>;

template <typename T> class TransactionExecutor {
    typedef ConcurrentQueue<Query<T> *> QueryQueue;

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
    void run(QueryQueue *queries) {
        auto rc = RCOK;
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        auto done = false;
        while (!done) {
            // get next query
            if (!queries->try_pop(chosen_query)) {
                done = true;
                continue;
            }

            assert(chosen_query != nullptr);

            // prepare manager
            global_txn_id = thread_id + thread_txn_id * FLAGS_threads;
            thread_txn_id++;
            manager->reset(global_txn_id);

            auto start_time = get_server_clock();
            rc = run_query(chosen_query);
            auto end_time = get_server_clock();
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
    txn_man *manager;
};

template <typename T> class OnlineBatchSchedulerV2 : public IScheduler<T> {
    typedef ConcurrentQueue<Query<T> *> QueryQueue;
    enum Phase { UNION, FIND, EXECUTE };

    struct TransactionBatch {
        Phase phase;
        uint64_t start_index;
        uint64_t end_index;
    };

    struct DataInfo {
        EpochValue root;
        EpochValue size;
    };

  public:
    OnlineBatchSchedulerV2(uint64_t num_threads, uint64_t max_batch_size, Database *db)
        : _num_threads(num_threads), _max_batch_size(max_batch_size), _db(db), _epoch(0),
          _core_map(), round_robin(0), done(false) {

        auto num_data_items = AccessIterator<T>::get_max_key();
        _data_info = new DataInfo[num_data_items];
        for (uint64_t i = 0; i < num_data_items; i++) {
            _data_info[i].root.Set(_epoch, reinterpret_cast<long>(&_data_info[i]));
            _data_info[i].size.Set(_epoch, 1L);
        }

        _executors = new TransactionExecutor<T>[_num_threads];
        for (uint64_t i = 0; i < _num_threads; i++) {
            _executors[i].initialize(i, _db);
        }

        pthread_mutex_init(&core_allocation_mutex, NULL);
    }

    void schedule(WorkloadLoader<T> *loader) {
        loader->get_queries(_batch, _max_size);

        printf("Batch Size: %lu\n", _max_size);
        prepare();

        _current_batch.start_index = 0;
        _current_batch.end_index = _max_batch_size;
        _current_batch.end_index =
            (_current_batch.end_index > _max_size) ? _max_size : _current_batch.end_index;
        _current_batch.phase = UNION;
        counter.Set(_epoch, (long)_num_threads);

        execute();

        if (STATS_ENABLE) {
            stats.print();
        }
    }

    ~OnlineBatchSchedulerV2() {}

  protected:
    const uint64_t _num_threads;
    const uint64_t _max_batch_size;
    Database *_db;
    DataInfo *_data_info;
    short _epoch;
    TransactionBatch _current_batch;
    ConcurrentHashMap<long, QueryQueue *> _core_map;
    ConcurrentQueue<QueryQueue *> _worklists;

    // Synchronization primitives
    EpochValue counter;

    // Used for allocation of cores
    int64_t round_robin;
    pthread_mutex_t core_allocation_mutex;

    // Input to the entire pipeline
    uint64_t _max_size;
    Query<T> *_batch;
    ReadWriteSet *_rwset_info;

    // To terminate the program
    volatile bool done;

    // Executors for each thread
    TransactionExecutor<T> *_executors;

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
        auto scheduler = (OnlineBatchSchedulerV2<T> *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        scheduler->compute_read_write_set(thread_id);
        return nullptr;
    }

    void compute_read_write_set(uint64_t thread_id) {
        double size_approx = _max_size / _num_threads;
        uint64_t size_per_thread = ceil(size_approx);
        uint64_t start_index = thread_id * size_per_thread;
        uint64_t end_index = (thread_id + 1) * size_per_thread;
        end_index = (end_index > _max_size) ? _max_size : end_index;

        // computing read write set
        for (auto i = start_index; i < end_index; i++) {
            _batch[i].obtain_rw_set(&(_rwset_info[i]));
        }
    }

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
        printf("Total Runtime : %lf secs\n", duration);
    }

    static void *execute_helper(void *ptr) {
        auto data = (ThreadLocalData *)ptr;
        auto scheduler = (OnlineBatchSchedulerV2<T> *)data->fields[0];
        auto thread_id = data->fields[1];
        set_affinity(thread_id);
        scheduler->run(thread_id);
        return nullptr;
    }

    void run(uint64_t thread_id) {
        auto start_time = get_server_clock();
        while (!done) {
            do_union(thread_id, _current_batch);

            done_wait(thread_id);

            do_find(thread_id, _current_batch);

            done_wait(thread_id);

            do_execute(thread_id);

            done_wait(thread_id);
        }
        auto end_time = get_server_clock();
        auto duration = (end_time - start_time);
        INC_STATS(thread_id, run_time, duration);
    }

    void do_union(uint64_t thread_id, TransactionBatch batch) {
        double size_approx = (batch.end_index - batch.start_index) / _num_threads;
        uint64_t size_per_thread = ceil(size_approx);
        uint64_t start_index = batch.start_index + (thread_id * size_per_thread);
        uint64_t end_index = batch.start_index + ((thread_id + 1) * size_per_thread);
        end_index = (end_index > batch.end_index) ? batch.end_index : end_index;

        auto start_time = get_server_clock();

        for (uint64_t t = start_index; t < end_index; t++) {
            ReadWriteSet *rwset = &(_rwset_info[t]);
            for (uint32_t i = 1; i < rwset->num_accesses; i++) {
                auto key1 = rwset->accesses[i - 1].key;
                auto key2 = rwset->accesses[i].key;
                auto data_info1 = &(_data_info[key1]);
                auto data_info2 = &(_data_info[key2]);
                Union(data_info1, data_info2);
            }
        }

        auto end_time = get_server_clock();
        auto duration = (end_time - start_time);
        INC_STATS(thread_id, time_union, duration);
    }

    void do_find(uint64_t thread_id, TransactionBatch batch) {
        unordered_map<long, QueryQueue *> local_core_map;
        double size_approx = (batch.end_index - batch.start_index) / _num_threads;
        uint64_t size_per_thread = ceil(size_approx);
        uint64_t start_index = batch.start_index + (thread_id * size_per_thread);
        uint64_t end_index = batch.start_index + ((thread_id + 1) * size_per_thread);
        end_index = (end_index > batch.end_index) ? batch.end_index : end_index;

        auto start_time = get_server_clock();

        for (uint64_t t = start_index; t < end_index; t++) {
            auto rwset = &(_rwset_info[t]);
            auto key = rwset->accesses[0].key;
            auto data_info = &(_data_info[key]);
            auto cc = Find(data_info);
            QueryQueue *query_queue = nullptr;
            auto iter = local_core_map.find(cc);
            if (iter == local_core_map.end()) {
                query_queue = GetQueue(cc);
                local_core_map[cc] = query_queue;
            } else {
                query_queue = iter->second;
            }
            query_queue->push(&(_batch[t]));
        }

        auto end_time = get_server_clock();
        auto duration = (end_time - start_time);
        INC_STATS(thread_id, time_find, duration);
    }

    void do_execute(uint64_t thread_id) {
        QueryQueue *query_queue;
        while (_worklists.try_pop(query_queue)) {
            _executors[thread_id].run(query_queue);
        }
    }

    void done_wait(uint64_t thread_id) {
        EpochValue current;
        current.Set(__sync_sub_and_fetch(&counter.word, 1));

        short old_epoch = current.GetEpoch();
        long old_counter = current.GetValue();
        if (old_counter == 0) {
            // move to the next phase
            move_to_next_phase();

            // create the next word with (e+1, n)
            EpochValue next_phase_word((short)(old_epoch + 1), (long)_num_threads);

            // replace counter value from (e, 0) to (e+1, n)
            auto success = counter.AtomicCompareAndSwap(current, next_phase_word);
            assert(success);
        } else {
            auto start_time = get_server_clock();
            // wait until epoch value has been updated
            while (current.GetEpoch() == old_epoch) {
                usleep(1);
                current.AtomicCopy(counter);
            }
            auto end_time = get_server_clock();
            auto duration = (end_time - start_time);
            INC_STATS(thread_id, time_blocked, duration);
        }
    }

    void move_to_next_phase() {
        switch (_current_batch.phase) {
        case UNION:
            _core_map.clear();
            assert(_worklists.empty());
            _current_batch.phase = FIND;
            break;
        case FIND:
            _current_batch.phase = EXECUTE;
            break;
        case EXECUTE:
            if (_current_batch.end_index < _max_size) {
                __sync_fetch_and_add(&_epoch, 1);
                _current_batch.start_index += _max_batch_size;
                _current_batch.end_index += _max_batch_size;
                _current_batch.end_index =
                    (_current_batch.end_index > _max_size) ? _max_size : _current_batch.end_index;
                _current_batch.phase = UNION;

            } else {
                done = true;
                return;
            }
            break;
        default:
            assert(false);
        }
    }

  private:
    long Find(DataInfo *info) {
        EpochValue old_root(info->root);
        if (old_root.IsEpoch(_epoch)) {
            auto current_root_info = reinterpret_cast<DataInfo *>(old_root.GetValue());
            if (current_root_info != info) {
                EpochValue new_root;
                new_root.Set(Find(current_root_info));
                if (old_root.word != new_root.word) {
                    info->root.AtomicCompareAndSwap(old_root, new_root);
                }
            }
            return info->root.word;
        } else {
            info->root.AtomicReset(_epoch, reinterpret_cast<long>(info));
            return Find(info);
        }
    }

    void Union(DataInfo *p, DataInfo *q) {
        // Find the roots of p and q
        EpochValue info1, info2;
        info1.Set(Find(p));
        info2.Set(Find(q));

        if (info1.word == info2.word) {
            return;
        }

        auto root1 = reinterpret_cast<DataInfo *>(info1.GetValue());
        auto root2 = reinterpret_cast<DataInfo *>(info2.GetValue());

        // reset size for current epoch
        if (!root1->size.IsEpoch(_epoch)) {
            root1->size.AtomicReset(_epoch, 1);
        }

        // reset size for current epoch
        if (!root2->size.IsEpoch(_epoch)) {
            root2->size.AtomicReset(_epoch, 1);
        }

        // merge two nodes based on size
        auto root1_size = root1->size.GetValue();
        auto root2_size = root2->size.GetValue();

        if (root1_size < root2_size) {
            if (root1->root.AtomicCompareAndSwap(info1, info2)) {
                __sync_fetch_and_add(&(root2->size.word), root1_size);
                return;
            }
        } else {
            if (root2->root.AtomicCompareAndSwap(info2, info1)) {
                __sync_fetch_and_add(&(root1->size.word), root2_size);
                return;
            }
        }

        Union(p, q);
    }

    QueryQueue *GetQueue(long word) {
        auto iter = _core_map.find(word);
        if (iter != _core_map.end()) {
            return iter->second;
        } else {
            auto query_queue = new QueryQueue();
            auto status = _core_map.insert(std::pair<long, QueryQueue *>(word, query_queue));
            if (status.second) {
                _worklists.push(query_queue);
                return query_queue;
            } else {
                delete query_queue;
                return GetQueue(word);
            }
        }
    }
};

#endif // DBX1000_ONLINE_BATCH_SCHEDULER_V2_H
