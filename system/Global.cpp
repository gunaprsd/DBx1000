#include "Allocator.h"
#include "dl_detect.h"
#include "Global.h"

#include "Manager.h"
#include "plock.h"
#include "occ.h"
#include "Query.h"
#include "Statistics.h"
#include "vll.h"

Allocator mem_allocator;
Statistics stats;
DL_detect dl_detector;
Manager * glob_manager;
Scheduler * query_queue;
Plock part_lock_man;
OCCManager occ_man;
#if CC_ALG == VLL
VLLMan vll_man;
#endif 

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

Time g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
uint32_t g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
Time g_timeout = TIMEOUT;
Time g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
uint32_t g_ts_batch_num = TS_BATCH_NUM;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
uint32_t g_cc_alg = CC_ALG;
Time g_query_intvl = QUERY_INTVL;
uint32_t g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_read_perc = READ_PERC;
double g_write_perc = WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
uint32_t g_part_cnt = PART_CNT;
uint32_t g_virtual_part_cnt = VIRTUAL_PART_CNT;
uint32_t g_thread_cnt = THREAD_CNT;
uint64_t g_synth_table_size = SYNTH_TABLE_SIZE;
uint32_t g_req_per_query = REQ_PER_QUERY;
uint32_t g_field_per_tuple = FIELD_PER_TUPLE;
uint32_t g_init_parallelism = INIT_PARALLELISM;

uint32_t g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;

double g_contention_perc = CONTENTION_PERC;
uint32_t g_pos_in_txn = POS_IN_TXN;
uint32_t g_txn_length = REQ_PER_QUERY;

map<string, string> g_params;

#if TPCC_SMALL
uint32_t g_max_items = 10000;
uint32_t g_cust_per_dist = 2000;
#else 
uint32_t g_max_items = 100000;
uint32_t g_cust_per_dist = 3000;
#endif
