#ifndef DBX1000_QUERY_H
#define DBX1000_QUERY_H

#include "global.h"

enum QueryType { YCSB_QUERY, TPCC_PAYMENT_QUERY, TPCC_NEW_ORDER_QUERY, TPCC_DELIVERY_QUERY };

struct BaseQuery {
    QueryType type;
};

template<typename T>
struct Query : public BaseQuery
{
    T params;
};


class BaseQueryList {
public:
    virtual BaseQuery * next() = 0;
    virtual bool        done() = 0;
		virtual void reset() = 0;
};

template<typename T>
class QueryList: public BaseQueryList {
public:
    QueryList() : BaseQueryList() {}
    void initialize(Query<T> *  queries, uint64_t num_queries) {
        this->queries = queries;
        this->num_queries = num_queries;
        this->current = 0;
    }

    BaseQuery * next() override {
        if(current < num_queries) {
            return & queries[current++];
        } else {
            return NULL;
        }
    }

    bool done() override {
      return (current == num_queries);
    }

		void reset() override {
			current = 0;
		};

protected:
    Query<T> * queries;
    uint64_t num_queries;
    uint64_t current;
};


class BaseQueryMatrix
{
public:
		virtual void initialize(uint32_t num_arrays, uint64_t num_queries_per_array) {
			this->num_arrays = num_arrays;
			this->num_queries_per_array = num_queries_per_array;
		}

		uint32_t num_arrays;
		uint64_t num_queries_per_array;
		virtual void get(uint32_t i, uint32_t j, BaseQuery * * query) = 0;
};

template<typename T>
class QueryMatrix: public BaseQueryMatrix
{
public:
		void initialize(Query<T> * * queries, uint32_t num_arrays, uint64_t num_queries_per_array) {
			BaseQueryMatrix::initialize(num_arrays, num_queries_per_array);
			this->queries = queries;
		}
		void get(uint32_t i, uint32_t j, BaseQuery * * query) override {  *query = & queries[i][j]; }
protected:
		Query<T> * * queries;
};



#endif //DBX1000_QUERY_H
