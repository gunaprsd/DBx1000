#pragma once 

#include "stdint.h"
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <mm_malloc.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <sstream>
#include <time.h> 
#include <sys/time.h>
#include <math.h>

#include "pthread.h"

#include "Config.h"
#include "dl_detect.h"
#include "Types.h"
#include "Statistics.h"
#ifndef NOGRAPHITE
#include "carbon_user.h"
#endif

using namespace std;

class Allocator;
class Statistics;
class DL_detect;
class Manager;
class Scheduler;
class Plock;
class OCCManager;
class VLLMan;

typedef uint32_t uint32_t;
typedef int32_t SInt32;
typedef uint64_t uint64_t;
typedef int64_t SInt64;

typedef uint64_t Time; // time stamp type

/******************************************/
// Global Data Structure 
/******************************************/
extern Allocator mem_allocator;
extern Statistics stats;
extern DL_detect dl_detector;
extern Manager * glob_manager;
extern Scheduler * query_queue;
extern Plock part_lock_man;
extern OCCManager occ_man;
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
extern uint32_t g_virtual_part_cnt;
extern uint32_t g_thread_cnt;
extern Time g_abort_penalty; 
extern bool g_central_man;
extern uint32_t g_ts_alloc;
extern bool g_key_order;
extern bool g_no_dl;
extern Time g_timeout;
extern Time g_dl_loop_detect;
extern bool g_ts_batch_alloc;
extern uint32_t g_ts_batch_num;

extern map<string, string> g_params;

// YCSB
extern uint32_t g_cc_alg;
extern Time g_query_intvl;
extern uint32_t g_part_per_txn;
extern double g_perc_multi_part;
extern double g_read_perc;
extern double g_write_perc;
extern double g_zipf_theta;
extern uint64_t g_synth_table_size;
extern uint32_t g_req_per_query;
extern uint32_t g_field_per_tuple;
extern uint32_t g_init_parallelism;

// EXPERIMENT
extern double g_contention_perc;
extern uint32_t g_pos_in_txn;
extern uint32_t g_txn_length;

// TPCC
extern uint32_t g_num_wh;
extern double g_perc_payment;
extern bool g_wh_update;
extern char * output_file;
extern uint32_t g_max_items;
extern uint32_t g_cust_per_dist;
