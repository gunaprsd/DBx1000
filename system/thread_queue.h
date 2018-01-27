#ifndef DBX1000_THREAD_QUEUE_H
#define DBX1000_THREAD_QUEUE_H

#include "global.h"
#include "helper.h"

/*
 * Each thread has a ThreadQueue that is responsible for picking up the next
 * query to process. Whenever a thread gets a query using the next_query
 * function, it must return the status before obtaining the next query. The
 * logic of obtaining the nextZipfInt64 query depends on whether the abort
 * buffer is enabled or not.
 *
 * Abort Buffer Enabled:
 * ---------------------
 * Each entry in the abort buffer has a ready_time that is computed based on a
 * penalty. An aborted transaction cannot be retried before this ready_time. If
 * abort buffer is empty, we simply skip this step. Else, we find the first
 * ready query If abort buffer is full and none of them are ready, we wait until
 * the least ready_time. If we cannot obtain any query from abort buffer, we get
 * it from our main array
 *
 * Abort Buffer Disabled:
 * ----------------------
 * When abort buffer is disabled, we retry a transaction until it is executed.
 * To reduce number of spurious aborts, we sleep for a penalty amount of time
 * before retrying.
 */

template <typename T> class ThreadQueue {
  struct AbortBufferEntry {
    ts_t ready_time;
    Query<T> *query;
  };

public:
  void initialize(uint64_t thread_id, QueryIterator<T> *query_list,
                  bool abort_buffer = true) {
    _thread_id = thread_id;
    srand48_r((_thread_id + 1) * get_sys_clock(), &_rand_buffer);

    _query_not_returned = false;
    _current_query = NULL;
    _previous_query = NULL;
    _previous_query_status = RCOK;

    _query_list = query_list;

    _abort_buffer_enable = abort_buffer;
    if (_abort_buffer_enable) {
      _abort_buffer_size = FLAGS_abort_buffer_size;
      _abort_buffer_empty_slots = FLAGS_abort_buffer_size;
      _abort_buffer = (AbortBufferEntry *)_mm_malloc(
          sizeof(AbortBufferEntry) * _abort_buffer_size, 64);
      ts_t init_time = get_sys_clock();
      for (uint32_t i = 0; i < _abort_buffer_size; i++) {
	_abort_buffer[i].ready_time = init_time;
        _abort_buffer[i].query = nullptr;
      }
    }
  }

  void reset() {
    _query_not_returned = false;
    _current_query = NULL;
    _previous_query = NULL;
    _previous_query_status = RCOK;
    if (_abort_buffer_enable) {
      _abort_buffer_size = FLAGS_abort_buffer_size;
      _abort_buffer_empty_slots = FLAGS_abort_buffer_size;
      ts_t init_time = get_sys_clock();
      for (uint32_t i = 0; i < _abort_buffer_size; i++) {
	_abort_buffer[i].ready_time = init_time;
        _abort_buffer[i].query = nullptr;
      }
    }
    _query_list->reset();
  }

  bool done() {
    // we are not done if a query is being executed
    bool finish = !_query_not_returned;
    // main array is done
    finish = finish && _query_list->done();
    // abort buffer must be empty if enabled
    finish = finish && (_abort_buffer_enable
                            ? (_abort_buffer_empty_slots == _abort_buffer_size)
                            : (_previous_query_status == RCOK));
    return finish;
  }

  Query<T> *next_query() {
    assert(_current_query == nullptr);

    while (true) {
      if (_abort_buffer_enable) {
        if (_abort_buffer_empty_slots < _abort_buffer_size) {
          int trial = 0;
          while (trial < 2) {

            // Look through the abort buffer for any ready query or compute min
            // ready time (when buffer is full)
            ts_t current_time = get_sys_clock();
            ts_t min_ready_time = UINT64_MAX;
            for (uint32_t i = 0; i < _abort_buffer_size; i++) {
              if (_abort_buffer[i].query != nullptr &&
                  current_time > _abort_buffer[i].ready_time) {
                _current_query = _abort_buffer[i].query;
                _abort_buffer[i].query = nullptr;
                _abort_buffer[i].ready_time = UINT64_MAX;
                _abort_buffer_empty_slots++;
                break;
              } else if (trial == 0 && _abort_buffer_empty_slots == 0 &&
                         _abort_buffer[i].ready_time < min_ready_time) {
                min_ready_time = _abort_buffer[i].ready_time;
              }
            }

            // sleep until you can at least one query is ready, if abort buffer
            // is full
            if (_abort_buffer_empty_slots == 0 && _current_query == nullptr) {
              assert(min_ready_time >= current_time);
              usleep(min_ready_time - current_time);
            } else if (_current_query != nullptr) {
              break;
            }

            trial++;
          }
        }

        // Obtain a query from the main array
        if (_current_query == nullptr) {
          _current_query = _query_list->next();
        }

      } else {
        // Take from main list only when previous txn status is OK
        if (_previous_query_status == RCOK) {
          _current_query = _query_list->next();
        } else {
          _current_query = _previous_query;
        }
      }

      if (_current_query != nullptr) {
        break;
      }
    }

    _query_not_returned = true;
    return _current_query;
  }

  void return_status(RC status) {
    _query_not_returned = false;
    _previous_query = _current_query;
    _previous_query_status = status;
    _current_query = NULL;

    if (_previous_query_status == Abort) {
      uint64_t penalty = 0;
      if (ABORT_PENALTY != 0) {
        double r;
        drand48_r(&_rand_buffer, &r);
        penalty = r * FLAGS_abort_penalty;
      }

      if (_abort_buffer_enable) {
        assert(_abort_buffer_empty_slots > 0);
        bool added = false;
        for (uint32_t i = 0; i < _abort_buffer_size; i++) {
          if (_abort_buffer[i].query == nullptr) {
            _abort_buffer[i].query = _previous_query;
            _abort_buffer[i].ready_time = get_sys_clock() + penalty;
            _abort_buffer_empty_slots--;
            added = true;
            break;
          }
        }
        assert(added);
      } else {
        usleep(penalty / 1000);
      }
    }
  }

protected:
  // Other data
  drand48_data _rand_buffer;
  uint64_t _thread_id;

  // Current status
  bool _query_not_returned;
  Query<T> *_current_query;
  Query<T> *_previous_query;
  RC _previous_query_status;

  // Main Queue fields
  QueryIterator<T> *_query_list;

  // Abort buffer fields
  bool _abort_buffer_enable;
  uint64_t _abort_buffer_size;
  uint64_t _abort_buffer_empty_slots;
  AbortBufferEntry *_abort_buffer;
};

#endif // DBX1000_THREAD_QUEUE_H
