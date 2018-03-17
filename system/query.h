#ifndef SYSTEM_QUERY_H__
#define SYSTEM_QUERY_H__

//#define CONNNECTED_COMP_FIELDS
#define SCHEDULER_TREE_FIELDS
#include "global.h"
#include <tbb/concurrent_queue.h>
#include <queue>

using namespace tbb;
enum QueryType { YCSB_QUERY, TPCC_PAYMENT_QUERY, TPCC_NEW_ORDER_QUERY };

/*
 * All queries derive from BaseQuery.
 * It mainly specifies the type of the query.
 */
struct Link {
    BaseQuery* next;
    double weight;
};

struct BaseQuery {
    QueryType type;
    int64_t core;
    int64_t num_links;
    Link links[MAX_NUM_ACCESSES];
};

struct AccessRecord {
    uint32_t table_id;
    uint64_t key;
    access_t access_type;
};

struct ReadWriteSet {
    uint32_t num_accesses;
    AccessRecord accesses[MAX_NUM_ACCESSES];
    ReadWriteSet() : num_accesses(0) {}
    void add_access(uint32_t tid, uint64_t key, access_t access_type) {
        accesses[num_accesses].table_id = tid;
        accesses[num_accesses].key = key;
        accesses[num_accesses].access_type = access_type;
	    num_accesses++;
    }
};

/*
 * Any query is of the form Query<T> where T is the set of params.
 */
template <typename T>
struct Query : public BaseQuery {
    T params;
#ifdef CONNNECTED_COMP_FIELDS
    pthread_mutex_t mutex;
    bool done_with_this;
    int64_t owner;
    queue<Query<T>*>* txn_queue;
    Query<T>* parent;
#endif
#ifdef SCHEDULER_TREE_FIELDS
	Query<T>* parent;
	Query<T>* next;
	Query<T>* head;
	Query<T>* children_roots[MAX_NUM_ACCESSES];
	int64_t num_active_children;
#endif
    void obtain_rw_set(ReadWriteSet* rwset);
};

/*
 * AccessIterator is used to iterate over various data items
 * Each query type has to implement the following functions
 * of the access iterator.
 */
template <typename T> class AccessIterator {
  public:
    void set_query(Query<T> *query);
    bool next(uint64_t &key, access_t &type, uint32_t &table_id);
    void set_cc_info(char cc_info);
    static uint64_t get_max_key();
    static uint32_t get_num_tables();
    static uint32_t max_access_per_txn(uint32_t table_id);

  protected:
    Query<T> *_query;
    uint32_t _current_req_id;
};

/*
 * QueryIterator is an abstraction that takes in an array of
 * queries and num_queries and provides an iterator interface over it.
 */
template <typename T> class QueryIterator {
  public:
    QueryIterator(Query<T> *queries, uint64_t num_queries)
        : _queries(queries), _num_queries(num_queries), _current(0) {}
    Query<T> *next() {
        if (_current < _num_queries) {
            return &_queries[_current++];
        } else {
            return nullptr;
        }
    }
    bool done() const { return (_current == _num_queries); }
    ~QueryIterator() = default;
    void reset() {
        _current = 0;
    }
  protected:
    Query<T> *const _queries;
    const uint64_t _num_queries;
    uint64_t _current;
};

/*
 * QueryMatrix is an abstraction used to refer to an array of
 * queries one for each thread.
 * num_cols = number of threads
 * num_rows = number of queries in each thread
 */
template <typename T> class QueryMatrix {
  public:
    QueryMatrix(Query<T> **queries, uint64_t num_cols, uint64_t num_rows) {
        this->num_cols = num_cols;
        this->num_rows = num_rows;
        this->queries = queries;
    }
    ~QueryMatrix() = default;
    uint64_t num_cols;
    uint64_t num_rows;
    Query<T> **queries;
};

/*
 * QueryBatch is another useful abstraction. Internally we have a
 * QueryMatrix and a QueryBatch is a frame on top of the matrix
 * from (0, frame_start) to (n-1, frame_end). The queries
 * are numbered in column-major format.
 */
template <typename T> class QueryBatch {
  public:
    QueryBatch(QueryMatrix<T> *matrix, uint64_t frame_start, uint64_t frame_end) {
        _matrix = matrix;
        _frame_start = frame_start;
        _frame_end = frame_end;
    }

    Query<T> *operator[](uint64_t index) {
        auto col_index = index / _matrix->num_cols;
        auto row_index = index % _matrix->num_cols;
        return &_matrix->queries[row_index][col_index];
    }
    uint64_t size() { return (_frame_end - _frame_start) * _matrix->num_cols; }

  protected:
    QueryMatrix<T> *_matrix;
    uint64_t _frame_start, _frame_end;
};

#ifdef CONNECTED_COMP_FIELDS
template<typename T>
Query<T> *find_root(Query<T> *node) {
    if (node == nullptr) {
        return nullptr;
    } else {
        auto parent = node->parent;
        auto root = node;
        while (parent != nullptr) {
            root = parent;
            parent = root->parent;
        }
        if (root->owner != -1) {
            return root;
        } else {
            return nullptr;
        }
    }
}
#endif


template<typename T>
using SharedQueryQueue = tbb::concurrent_queue<Query<T> *>;


template<typename T>
class ITransactionQueue {
public:
	virtual bool next(int32_t thread_id, Query<T> *&txn) = 0;
	virtual void add(Query<T> *txn, int32_t thread_id = -1) = 0;
	virtual void load_all(QueryIterator<T>* iterator, int32_t thread_id) {
		while (!iterator->done()) {
			Query<T> *query = iterator->next();
			add(query, thread_id);
		}
	}
};

#endif // SYSTEM_QUERY_H__
