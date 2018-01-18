#ifndef DBX1000_ONLINEPARTITIONER_H
#define DBX1000_ONLINEPARTITIONER_H


#include <cstdint>

class BaseQuery;

class Scheduler {
public:
		virtual void partition_next_batch() = 0;
		virtual BaseQuery* get_next_query(uint32_t tid) = 0;
};

#endif //DBX1000_ONLINEPARTITIONER_H
