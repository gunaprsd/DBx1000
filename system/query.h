#ifndef DBX1000_QUERY_H
#define DBX1000_QUERY_H

#include "global.h"

enum QueryType {
  YCSB_QUERY,
  TPCC_PAYMENT_QUERY,
  TPCC_NEW_ORDER_QUERY,
  TPCC_DELIVERY_QUERY
};

/*
 * All queries derive from BaseQuery.
 * It mainly specifies the type of the query.
 */
struct BaseQuery {
  QueryType type;
};

/*
 * Any query is of the form Query<T> where T is the set of params.
 */
template <typename T> struct Query : public BaseQuery { T params; };


/*
 * AccessIterator is used to iterate over various data items
 */
template<typename T>
class AccessIterator {
public:
    void setQuery(Query<T>* query);
    bool getNextAccess(uint64_t & key, access_t & type);
    static uint64_t getMaxKey();
protected:
		Query<T> *_query;
		uint32_t _current_req_id;
};

template <typename T> class QueryList {
public:
  QueryList()  {}
  void initialize(Query<T> *queries, uint64_t num_queries) {
    this->queries = queries;
    this->num_queries = num_queries;
    this->current = 0;
  }
  Query<T> *next() {
    if (current < num_queries) {
      return &queries[current++];
    } else {
      return NULL;
    }
  }
  bool done() { return (current == num_queries); }
  void reset() { current = 0; };
protected:
  Query<T> *queries;
  uint64_t num_queries;
  uint64_t current;
};

template <typename T> class QueryMatrix {
public:
  void initialize(Query<T> **queries, uint32_t num_arrays,
                  uint64_t num_queries_per_array) {
    this->num_arrays = num_arrays;
		this->num_queries_per_array = num_queries_per_array;
    this->queries = queries;
  }
  void get(uint32_t i, uint32_t j, Query<T> **query) {
    *query = &queries[i][j];
  }
	uint32_t num_arrays;
	uint64_t num_queries_per_array;
	Query<T> **queries;
protected:
};

template <typename T>
class QueryBatch {
public:
		QueryBatch(QueryMatrix<T>* matrix, uint64_t frame_start, uint64_t frame_end) {
			_matrix = matrix;
			_frame_start = frame_start;
			_frame_end = frame_end;
		}
		Query<T>* operator[](uint64_t index) {
			auto col_index = index/_matrix->num_arrays;
			auto row_index = index % _matrix->num_arrays;
			return & _matrix->queries[row_index][col_index];
		}
		uint64_t size() {
			return (_frame_end - _frame_start)* _matrix->num_arrays;
		}
protected:
		QueryMatrix<T>* _matrix;
		uint64_t _frame_start, _frame_end;
};

#endif // DBX1000_QUERY_H
