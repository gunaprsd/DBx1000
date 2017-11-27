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

#endif //DBX1000_QUERY_H
