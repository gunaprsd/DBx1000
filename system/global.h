#ifndef __GLOBAL_H__
#define __GLOBAL_H__

#include "stdint.h"
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <math.h>
#include <metis.h>
#include <mm_malloc.h>
#include <set>
#include <sstream>
#include <stdio.h>
#include <string>
#include <sys/time.h>
#include <time.h>
#include <typeinfo>
#include <unistd.h>
#include <vector>

#include "config.h"
#include "dl_detect.h"
#include "pthread.h"
#include "stats.h"
#include "parser.h"
#ifndef NOGRAPHITE
#include "carbon_user.h"
#endif

#define SELECTIVE_CC
//#define PRINT_CLUSTERED_FILE
using namespace std;

class mem_alloc;
class Stats;
class DL_detect;
class Manager;
class Plock;
class OptCC;
class VLLMan;

typedef uint32_t uint32_t;
typedef int32_t SInt32;
typedef uint64_t uint64_t;
typedef int64_t SInt64;

typedef int32_t int32;
typedef int64_t int64;
typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure
/******************************************/
extern mem_alloc mem_allocator;
extern Stats stats;
extern DL_detect dl_detector;
extern Manager *glob_manager;
extern Plock part_lock_man;
extern OptCC occ_man;
#if CC_ALG == VLL
extern VLLMan vll_man;
#endif

extern bool volatile warmup_finish;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
extern carbon_barrier_t enable_barrier;
#endif

/******************************************/
// Global Parameter
/******************************************/
extern bool g_part_alloc;
extern bool g_mem_pad;
extern bool g_prt_lat_distr;
extern uint32_t g_part_cnt;

extern bool g_central_man;
extern uint32_t g_ts_alloc;
extern bool g_key_order;
extern bool g_no_dl;
extern ts_t g_timeout;
extern ts_t g_dl_loop_detect;
extern bool g_ts_batch_alloc;
extern uint32_t g_ts_batch_num;

// YCSB
extern uint32_t g_cc_alg;
extern ts_t g_query_intvl;


// TPCC
extern uint32_t g_num_wh;
extern double g_perc_payment;
extern char *output_file;

enum RC { RCOK, Commit, Abort, WAIT, ERROR, FINISH };

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t;  // row id
typedef uint64_t pgid_t; // page id

/* INDEX */
enum latch_t { LATCH_EX, LATCH_SH, LATCH_NONE };
// accessing type determines the latch type on nodes
enum idx_acc_t { INDEX_INSERT, INDEX_READ, INDEX_NONE };
typedef uint64_t idx_key_t;              // key id for index
typedef uint64_t (*func_ptr)(idx_key_t); // part_id func_ptr(index_key);

/* general concurrency control */
enum access_t { RD, WR, XP, SCAN };
/* LOCK */
enum lock_t { LOCK_EX, LOCK_SH, LOCK_NONE };
/* TIMESTAMP */
enum TsType { R_REQ, W_REQ, P_REQ, XP_REQ };

#define MSG(str, args...)                                                      \
  { printf("[%s : %d] " str, __FILE__, __LINE__, args); }                      \
//	printf(args); }

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should
// use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX index_btree
#else // IDX_HASH
#define INDEX IndexHash
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 18446744073709551615UL
#endif // UINT64_MAX

struct ThreadLocalData {
  uint64_t fields[8];
};

#define WRITE_PARTITIONS_TO_FILE false
#define PRINT_PARTITION_SUMMARY true

class BaseQuery;
void print_query(FILE *file, BaseQuery *query);


/* PRE_PROCESSING */
enum TaskType {
  GENERATE,
  PARTITION_DATA,
  PARTITION_CONFLICT,
  EXECUTE_RAW,
  EXECUTE_PARTITIONED
};
extern uint64_t g_size_per_thread;
extern uint64_t g_size;
extern TaskType g_task_type;

extern char *g_benchmark;
extern char *g_benchmark_tag;
extern char *g_benchmark_tag2;
extern int g_ufactor;

extern string g_data_folder;

template <typename T>
string get_table_name(uint32_t id);

#endif
