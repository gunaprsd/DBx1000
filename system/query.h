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
};

template<typename T>
class QueryList: public BaseQueryList {
public:
    void initialize(Query<T> *  queries, uint32_t num_queries) {
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
        return current == num_queries;
    }

protected:
    Query<T> * queries;
    uint32_t num_queries;
    uint32_t current;
};


#endif //DBX1000_QUERY_H
