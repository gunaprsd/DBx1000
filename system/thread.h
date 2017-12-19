#ifndef DBX1000_THREAD_H
#define DBX1000_THREAD_H

#include "database.h"
#include "query.h"
#include "thread_queue.h"

class Thread {
public:
    void 		    initialize(uint32_t id, Database * db, BaseQueryList * query_list, bool abort_buffer_enable = true);
    RC 			    run();

private:
    uint64_t        thread_id;
    Database *      db;
    BaseQueryList * query_list;
    ts_t 		    _curr_ts;
    ts_t 		get_next_ts();
};

class BenchmarkExecutor {
public:
    virtual void    initialize		(uint32_t num_threads, const char * path);
		virtual	void 		execute				();
		virtual	void 		release				();
protected:
		static 	void *  execute_helper(void * data);
    Thread *    		_threads;
    uint32_t    		_num_threads;
		char 						_path[200];
};

#endif //DBX1000_THREAD_H
