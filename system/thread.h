#ifndef DBX1000_THREAD_H
#define DBX1000_THREAD_H

#include "database.h"
#include "query.h"
#include "thread_queue.h"

class Thread {
public:
    void 		    initialize(uint32_t id, Database * db, BaseQueryList * query_list, uint64_t num_queries, bool abort_buffer_enable = true);
    RC 			    run();


private:
    uint64_t        thread_id;
    Database *      db;
    ThreadQueue *   query_queue;

    ts_t 		    _curr_ts;
    ts_t 		get_next_ts();
};

class BenchmarkExecutor {
public:
    virtual void    initialize(uint32_t num_threads);
    void execute();
    static void *   execute_helper(void *);
protected:
    Thread *    _threads;
    uint32_t    _num_threads;
};

#endif //DBX1000_THREAD_H
