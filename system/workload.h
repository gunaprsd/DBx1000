#include "query.h"
#include "global.h"
#include "helper.h"

#ifndef DBX1000_WORKLOAD_H
#define DBX1000_WORKLOAD_H

/*
 * ParallelWorkloadGenerator:
 * --------------------------
 * Generates a workload (in parallel) based on config and command line arguments
 * If a base_file_name is specified, the queries are written onto k binary files
 * with the format <base_file_name>_<i>.dat
 */
class ParallelWorkloadGenerator
{
public:
    virtual void    initialize(uint32_t num_threads, uint64_t num_params_per_thread, const char * base_file_name = NULL);
            void    generate();
    virtual void    release();

    virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
protected:
    static  void *  run_helper(void *ptr);
    virtual void    per_thread_generate(uint32_t thread_id) = 0;
    virtual void    per_thread_write_to_file(uint32_t thread_id, FILE * file) = 0;

    //Data fields
    uint64_t    _num_params;
    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    char *      _base_file_name;
    bool        _write_to_file;
};

/*
 * ParallelWorkloadLoader:
 * ------------------------
 * Loads k binary files of the form <base_file_name>_<i>.dat that each contain
 * queries in the binary format. The loaded queries can be obtained using
 * get_queries_list.
 */
class ParallelWorkloadLoader
{
public:
    virtual void    initialize(uint32_t num_threads, uint64_t num_params_per_thread, char * base_file_name);
            void    load();
    virtual void    release();

    virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
protected:
    static  void *          run_helper(void *ptr);
    virtual void            per_thread_load(uint32_t thread_id, FILE * file) = 0;


    //Data fields
    uint64_t    _num_params;
    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    char *      _base_file_name;
};


/*
 * OfflineWorkloadPartitioner:
 * -------------------
 * Partitions a given workload from k files <base_file_name>_<i>.dat
 * and writes back partitioned_<base_file_name>_<i>.dat
 */
class OfflineWorkloadPartitioner /* Single Threaded */
{
public:
    virtual void    initialize(uint32_t num_threads, uint64_t num_params_per_thread, uint64_t num_params_pgpt,  const char * base_file_name);
            void    partition();
    virtual void    release();
protected:
            void    open_all_files();
            void    close_all_files();
    virtual void    partition_workload_part(uint32_t iteration, uint64_t num_records) = 0;

    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    uint64_t    _num_params_pgpt;

    char *      _base_file_name;
    FILE * *    _files;
    FILE * *    _out_files;

    uint32_t num_iterations;
    double read_duration;
    double write_duration;
    double data_statistics_duration;
    double graph_init_duration;
    double partition_duration;
};


class WorkloadPartitioner
{
public:
    virtual void initialize(uint32_t num_threads, uint64_t num_params_per_thread, uint64_t num_params_pgpt, ParallelWorkloadGenerator * generator);
    virtual void partition();
    virtual void release();
    virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
protected:
    virtual void partition_workload_part(uint32_t iteration, uint64_t num_records) = 0;

    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    uint64_t    _num_params_pgpt;

    ParallelWorkloadGenerator * _generator;

    uint32_t num_iterations;
    double data_statistics_duration;
    double graph_init_duration;
    double partition_duration;
    double shuffle_duration;
};
#endif //DBX1000_WORKLOAD_H
