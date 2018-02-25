
#ifndef DBX1000_THREAD_NEW_H
#define DBX1000_THREAD_NEW_H

#include "database.h"
#include "manager.h"
#include "query.h"
#include "thread_queue.h"
#include "txn.h"
#include "vll.h"
#include <functional>
#include <queue>
#include <tuple>
#define QUEUE_SIZE 100
using namespace std;

template <typename T> class BFSQueue {
    Query<T> *items[QUEUE_SIZE];
    int32_t head, tail;

  public:
    BFSQueue() {
        head = 0;
        tail = 0;
        for (uint32_t i = 0; i < QUEUE_SIZE; i++) {
            items[i] = nullptr;
        }
    }

    void clear() { head = tail = 0; }

    bool pop(Query<T> *&query) {
        if (head == tail) {
            query = nullptr;
            return false;
        } else {
            assert(head > tail);
            query = items[tail % QUEUE_SIZE];
            tail++;
            return true;
        }
    }

    bool push(Query<T> *query) {
        if ((head - tail) < QUEUE_SIZE) {
            items[head % QUEUE_SIZE] = query;
            head++;
            return true;
        } else {
            // done with size
            return false;
        }
    }
};
typedef tuple<uint64_t, BaseQuery *> ptype;
auto cmp = [](ptype t1, ptype t2) { return get<0>(t1) < get<0>(t2); };

template <typename T> class TimedAbortBuffer {
    priority_queue<ptype, vector<ptype>, decltype(cmp)> priorityQueue;
    RandomNumberGenerator gen;

  public:
    TimedAbortBuffer() : priorityQueue(cmp), gen(1) {}

    void add_query(Query<T> *query) {
        uint64_t penalty = 0;
        if (ABORT_PENALTY != 0) {
            double r = gen.nextDouble(0);
            penalty = static_cast<uint64_t>(r * FLAGS_abort_penalty);
        }
        uint64_t ready_time = get_sys_clock() + penalty;
        auto t = make_tuple(ready_time, (BaseQuery *)query);
        priorityQueue.push(t);
    }

    bool get_ready_query(Query<T> *&query) {
        if (priorityQueue.empty()) {
            query = nullptr;
            return false;
        } else {

            // ensure you wait and get a query from buffer
            // if it is getting filled up beyond buffer size
            while (priorityQueue.size() >= (size_t)FLAGS_abort_buffer_size) {
                auto current_time = get_sys_clock();
                auto top_time = get<0>(priorityQueue.top());
                if (current_time < top_time) {
                    usleep(top_time - current_time);
                } else {
                    query = reinterpret_cast<Query<T> *>(get<1>(priorityQueue.top()));
                    priorityQueue.pop();
                    return true;
                }
            }

            // remove a query from buffer only if it is ready
            auto current_time = get_sys_clock();
            auto top_time = get<0>(priorityQueue.top());
            if (top_time < current_time) {
                query = reinterpret_cast<Query<T> *>(get<1>(priorityQueue.top()));
                priorityQueue.pop();
                return true;
            } else {
                query = nullptr;
                return false;
            }
        }
    }

    bool empty() { return priorityQueue.empty(); }
};

template <typename T> class CCThread {
  public:
    void initialize(uint32_t id, Database *db, QueryIterator<T> *query_list,
                    bool abort_buffer_enable = true) {
        this->thread_id = id;
        this->db = db;
        this->query_iterator = query_list;
	    this->manager = db->get_txn_man(thread_id);
        this->bfs_queue = new BFSQueue<T>();
        this->abort_buffer = new TimedAbortBuffer<T>();
	    glob_manager->set_txn_man(manager);
	    stats.init(thread_id);
    }

    void connect() {
        ReadWriteSet rwset;
        query_iterator->reset();
        uint64_t cnt = 0;
        while (!query_iterator->done()) {
            Query<T> *query = query_iterator->next();
            query->core = 0;
            query->obtain_rw_set(&rwset);
            query->num_links = rwset.num_accesses;
            for (uint64_t i = 0; i < rwset.num_accesses; i++) {
                bool added = false;
                query->links[i].next = nullptr;
                BaseQuery **outgoing_loc = &(query->links[i].next);
                BaseQuery **incoming_loc =
                    reinterpret_cast<BaseQuery **>(db->data_next_pointer[rwset.accesses[i].key]);
                while (!added) {
                    BaseQuery ***incoming_loc_loc = &incoming_loc;
                    if (__sync_bool_compare_and_swap(incoming_loc_loc, incoming_loc,
                                                     outgoing_loc)) {
                        added = true;
                        cnt++;
                        if (incoming_loc != 0) {
                            assert(*incoming_loc == nullptr);
                            *incoming_loc = query;
                        }
                    }
                }
            }
        }
        PRINT_INFO(lu, "EdgeCount", cnt);
        query_iterator->reset();
    }

    RC run_query(txn_man* m_txn, Query<T>* m_query) {
        RC rc = RCOK;
        ts_t start_time = get_sys_clock();
        INC_STATS(thread_id, time_query, get_sys_clock() - start_time);

        // Step 2: Prepare the transaction manager
        global_txn_id = thread_id + thread_txn_id * FLAGS_threads;
        thread_txn_id++;
        m_txn->reset(global_txn_id);

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
        rc = m_txn->run_txn(m_query);
#elif CC_ALG == OCC
        m_txn->start_ts = get_next_ts();
            rc = m_txn->run_txn(m_query);
#elif CC_ALG == TIMESTAMP
            rc = m_txn->run_txn(m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
            m_txn->set_ts(get_next_ts());
            glob_manager->add_ts(thread_id, m_txn->get_ts());
            rc = m_txn->run_txn(m_query);
#elif CC_ALG == HSTORE
            if (!HSTORE_LOCAL_TS) {
                m_txn->set_ts(get_next_ts());
            }
            // rc = part_lock_man.lock(m_txn, m_query->part_to_access,
            // m_query->part_num);
            if (rc == RCOK) {
                m_txn->run_txn(m_query);
            }
            // part_lock_man.unlock(m_txn, m_query->part_to_access,
            // m_query->part_num);
#elif CC_ALG == VLL
            vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == SILO
            rc = m_txn->run_txn(m_query);
#endif
        ts_t end_time = get_sys_clock();

        // Step 5: Update statistics
        uint64_t duration = end_time - start_time;
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
        return rc;
    }

    RC run() {
        auto rc = RCOK;
        auto chosen_query = static_cast<Query<T> *>(nullptr);
        bool done = false;
        bool prev_query_successful = false;

        while(!done) {
            /*
             * We have three places to look for the next query.
             * 1. First priority is for abort buffer.
             *
             * 2. If the previous query was successful and bfs_queue
             * is not empty, we execute the next query in the queue.
             *
             * 3. If there was an abort, then we clear the bfs_queue
             * and pick one up either from the query_iterator.
             */

            chosen_query = nullptr;
            if(!abort_buffer->get_ready_query(chosen_query)) {
                // no query in abort buffer
                if(prev_query_successful) {
                    bfs_queue->pop(chosen_query);
                } else {
                    bfs_queue->clear();
                }
            }

            // if nothing obtained from abort buffer or
            // bfs queue, then get the next txn in input queue
            if(chosen_query == nullptr) {
                if(query_iterator->done()) {
	                if(abort_buffer->empty()) {
		                done = true;
	                }
                    continue;
                } else {
                    chosen_query = query_iterator->next();
                }
            }

            assert(chosen_query != nullptr);

            // set core of txn
            if (!ATOM_CAS(chosen_query->core, 0, thread_id + 1)) {
                // already handled by another core, skip!
                continue;
            }


            // execute query
            rc = run_query(manager, chosen_query);

            // add to abort buffer queue, if aborted
            if(rc == Abort) {
                abort_buffer->add_query(chosen_query);
                prev_query_successful = false;
            } else if(rc == RCOK) {
                prev_query_successful = true;
            } else {
                assert(false);
            }
        }

        return FINISH;
    }

  private:
    uint64_t thread_id;
    uint64_t global_txn_id;
    uint64_t thread_txn_id;
    Database *db;
    QueryIterator<T> *query_iterator;
	txn_man* manager;
    TimedAbortBuffer<T>* abort_buffer;
    BFSQueue<T>* bfs_queue;
    ts_t _curr_ts;
    ts_t get_next_ts() {
        if (g_ts_batch_alloc) {
            if (_curr_ts % g_ts_batch_num == 0) {
                _curr_ts = glob_manager->get_ts(thread_id);
                _curr_ts++;
            } else {
                _curr_ts++;
            }
            return _curr_ts - 1;
        } else {
            _curr_ts = glob_manager->get_ts(thread_id);
            return _curr_ts;
        }
    }
};

template <typename T> class Scheduler {
  public:
    Scheduler(const string &folder_path, uint64_t num_threads) {
        _num_threads = num_threads;
        _folder_path = folder_path;
        _threads = (CCThread<T> *)_mm_malloc(sizeof(CCThread<T>) * _num_threads, 64);
    }

    virtual void execute() {
        pthread_t threads[_num_threads];
        ThreadLocalData data[_num_threads];

        uint64_t start_time = get_server_clock();
        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, connect_helper, (void *)&data[i]);
        }

        for (uint32_t i = 0; i < _num_threads; i++) {
            pthread_join(threads[i], nullptr);
        }

        uint64_t end_time = get_server_clock();
        double duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
        printf("Connect Time : %lf secs\n", duration);

        start_time = get_server_clock();
        for (uint64_t i = 0; i < _num_threads; i++) {
            data[i].fields[0] = (uint64_t)this;
            data[i].fields[1] = (uint64_t)i;
            pthread_create(&threads[i], nullptr, execute_helper, (void *)&data[i]);
        }

        for (uint32_t i = 0; i < _num_threads; i++) {
            pthread_join(threads[i], nullptr);
        }

        end_time = get_server_clock();
        duration = ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
        printf("Total Runtime : %lf secs\n", duration);
        if (STATS_ENABLE) {
            stats.print();
        }
    }

    virtual void release() {}

  protected:
    static void *execute_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto executor = (Scheduler *)data->fields[0];
        auto thread_id = data->fields[1];
	    set_affinity(thread_id);

        executor->_threads[thread_id].run();
        return nullptr;
    }

    static void *connect_helper(void *ptr) {
        // Threads must be initialize before
        auto data = (ThreadLocalData *)ptr;
        auto executor = (Scheduler *)data->fields[0];
        auto thread_id = data->fields[1];

        executor->_threads[thread_id].connect();
        return nullptr;
    }

    uint64_t _num_threads;
    string _folder_path;
    CCThread<T> *_threads;
};

#endif // DBX1000_THREAD_NEW_H
