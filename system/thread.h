#ifndef DBX1000_THREAD_H
#define DBX1000_THREAD_H

#include "database.h"
#include "manager.h"
#include "query.h"
#include "thread_queue.h"
#include "txn.h"

template <typename T> class Thread {
public:
  void initialize(uint32_t id, Database *db, QueryIterator<T> *query_list,
                  bool abort_buffer_enable = true) {
    this->thread_id = id;
    this->db = db;
    this->query_iterator = query_list;
  }

  RC run() {
    stats.init(thread_id);
    set_affinity(thread_id);

    RC rc = RCOK;
    txn_man *m_txn = db->get_txn_man(thread_id);
    glob_manager->set_txn_man(m_txn);

    Query<T> *m_query = nullptr;
    uint64_t thd_txn_id = 0;

    ThreadQueue<T> *query_queue = nullptr;
    query_queue = new ThreadQueue<T>();
    query_queue->initialize(thread_id, query_iterator, FLAGS_abort_buffer);
    while (!query_queue->done()) {
      ts_t start_time = get_sys_clock();

      // Step 1: Obtain a query from the query queue
      m_query = query_queue->next_query();
      INC_STATS(thread_id, time_query, get_sys_clock() - start_time);

      // Step 2: Prepare the transaction manager
      uint64_t global_txn_id = thread_id + thd_txn_id * FLAGS_threads;
      thd_txn_id++;
      m_txn->reset(global_txn_id);

      // Step 3: Execute the transaction
      rc = RCOK;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
      rc = m_txn->run_txn(m_query);
#elif CC_ALG == OCC
      m_txn->start_ts = get_next_ts();
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

      // Step 4: Return status to query queue
      query_queue->return_status(rc);

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
    }

    delete query_queue;
    return FINISH;
  }

private:
  uint64_t thread_id;
  Database *db;
  QueryIterator<T> *query_iterator;
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

template <typename T> class BenchmarkExecutor {
public:
  BenchmarkExecutor(const string &folder_path, uint64_t num_threads) {
    _num_threads = num_threads;
    _folder_path = folder_path;
    _threads = (Thread<T> *)_mm_malloc(sizeof(Thread<T>) * _num_threads, 64);
  }

  virtual void execute() {
    pthread_t threads[_num_threads];
    ThreadLocalData data[_num_threads];

    uint64_t start_time = get_server_clock();
    for (uint64_t i = 0; i < _num_threads; i++) {
      data[i].fields[0] = (uint64_t)this;
      data[i].fields[1] = (uint64_t)i;
      pthread_create(&threads[i], nullptr, execute_helper, (void *)&data[i]);
    }

    for (uint32_t i = 0; i < _num_threads; i++) {
      pthread_join(threads[i], nullptr);
    }

    uint64_t end_time = get_server_clock();
    double duration =
        ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
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
    auto executor = (BenchmarkExecutor *)data->fields[0];
    auto thread_id = data->fields[1];

    executor->_threads[thread_id].run();
    return nullptr;
  }
  uint64_t _num_threads;
  string _folder_path;
  Thread<T> *_threads;
};

#endif // DBX1000_THREAD_H
