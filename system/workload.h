#include "query.h"
#include "global.h"
#include "helper.h"

#ifndef DBX1000_WORKLOAD_GENERATOR_H
#define DBX1000_WORKLOAD_GENERATOR_H


class WorkloadGenerator {
public:
    void    generate();
    virtual void    initialize(uint32_t num_threads, uint64_t num_params_per_thread, const char * base_file_name);
    static  void *    run_helper(void *ptr);

    //The following must be implemented by the user
    virtual BaseQueryList *     get_queries_list(uint32_t thread_id) = 0;
protected:
    virtual void            per_thread_generate(uint32_t thread_id) = 0;
    virtual void            per_thread_write_to_file(uint32_t thread_id, FILE * file) = 0;

    //Data fields
    uint64_t    _num_params;
    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    char *      _base_file_name;
    bool        _write_to_file;
};

class WorkloadLoader {
public:
    virtual void    initialize(uint32_t num_threads, uint64_t num_params, char * base_file_name);
    void            load();
    static  void *   run_helper(void *ptr);

    //The following must be implemented by the benchmark
    virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
protected:
    virtual void per_thread_load(uint32_t thread_id, FILE * file) = 0;

    //Data fields
    uint64_t    _num_params;
    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    char *      _base_file_name;
};



#endif //DBX1000_WORKLOAD_GENERATOR_H
