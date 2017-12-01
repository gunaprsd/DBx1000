#include "query.h"
#include "global.h"
#include "helper.h"
#include <vector>
#include "graph_partitioner.h"

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
    virtual void    initialize  (uint32_t num_threads,
                                 uint64_t num_params_per_thread,
                                 const char * base_file_name = NULL);
    virtual void    release     ();
            void    generate    ();
protected:
    static  void *  run_helper(void *ptr);

    uint64_t    _num_params;
    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    char *      _base_file_name;
    bool        _write_to_file;

/* Need to be implemented by benchmark */
public:
    virtual BaseQueryList *     get_queries_list(uint32_t thread_id) = 0;
    virtual BaseQueryMatrix *   get_queries_matrix() = 0;
protected:
    virtual void    per_thread_generate(uint32_t thread_id) = 0;
    virtual void    per_thread_write_to_file(uint32_t thread_id, FILE * file) = 0;
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
    virtual void                initialize  (uint32_t num_threads, char * base_file_name);
    virtual void                release     ();
            void                load        ();
protected:
    static  void *              run_helper  (void *ptr);

    uint32_t    _num_threads;
    char *      _base_file_name;

/* Need to be implemented by benchmark */
public:
    virtual BaseQueryList *     get_queries_list(uint32_t thread_id) = 0;
protected:
    virtual void            per_thread_load(uint32_t thread_id, FILE * file) = 0;
};

/*
 * WorkloadPartitioner:
 * --------------------
 * Partitions a given workload using the METIS graph partitioner
 * Any benchmark has to implement a compute weight function that is invoked
 * for every pair of transactions.
 */
class WorkloadPartitioner
{
public:
    virtual void   initialize                   (uint32_t num_threads,
                                                 uint64_t num_params_per_thread,
                                                 uint64_t num_params_pgpt,
                                                 ParallelWorkloadGenerator * generator);
    virtual void    partition                   ();
    virtual void    release                     ();
protected:
    virtual void    compute_data_info          (uint32_t iteration, uint64_t num_records);
            void    partition_workload_part    (uint32_t iteration, uint64_t num_records);
            void    write_pre_partition_file   (uint32_t iteration, uint64_t num_records);
            void    write_post_partition_file  (uint32_t iteration, uint64_t num_records);

    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    uint64_t    _num_params_pgpt;

    ParallelWorkloadGenerator * _generator;
    std::vector<BaseQuery*>*    _tmp_queries;
    uint32_t *                  _tmp_array_sizes;
    BaseQueryMatrix *           _orig_queries;

    uint32_t num_iterations;
    double data_statistics_duration;
    double graph_init_duration;
    double partition_duration;
    double shuffle_duration;


/** Need to be implemented by benchmarks */
public:
    virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
protected:
    virtual int             compute_weight(BaseQuery * q1, BaseQuery * q2) = 0;
};


#endif //DBX1000_WORKLOAD_H
