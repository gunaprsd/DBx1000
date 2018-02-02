

#include "global.h"
#include "dl_detect.h"
#include "manager.h"
#include "mem_alloc.h"
#include "occ.h"
#include "plock.h"
#include "query.h"
#include "stats.h"
#include "tpcc.h"
#include "vll.h"
#include "ycsb.h"
#include "parser.h"

mem_alloc mem_allocator;
Stats stats;
DL_detect dl_detector;
Manager *glob_manager;
Plock part_lock_man;
OptCC occ_man;
#if CC_ALG == VLL
VLLMan vll_man;
#endif

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

ts_t g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
uint32_t g_ts_alloc = TS_ALLOC;
bool g_no_dl = NO_DL;
ts_t g_timeout = TIMEOUT;
ts_t g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
uint32_t g_ts_batch_num = TS_BATCH_NUM;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;

bool g_prt_lat_distr = PRT_LAT_DISTR;
uint32_t g_part_cnt = PART_CNT;

char *output_file = NULL;




void print_ycsb_query(FILE *file, ycsb_query *query) {
  for (uint64_t k = 0; k < query->params.request_cnt; k++) {
    fprintf(file, "Sno\t:%lu\n", k);
    fprintf(file, "\tKey\t:%ld\n", (long int)query->params.requests[k].key);
    fprintf(file, "\tPartition-Key\t:%ld\n", query->params.requests[k].key / FLAGS_ycsb_num_partitions);
    fprintf(file, "\tPartition\t:%ld\n", query->params.requests[k].key % FLAGS_ycsb_num_partitions);
    fprintf(file, "\tCC_Info\t:%d\n", (int)query->params.requests[k].cc_info);
  }
  fprintf(file, "\n");
}

void print_tpcc_query(FILE *file, tpcc_query *query) {
  if (query->type == TPCC_PAYMENT_QUERY) {
    auto params = (tpcc_payment_params *)(&query->params);
    fprintf(file, "\tw_id\t:%lu\n", params->w_id);
    fprintf(file, "\td_id\t:%lu\n", params->d_id);
    fprintf(file, "\tc_id\t:%lu\n", params->c_id);
    fprintf(file, "\td_w_id\t:%lu\n", params->d_w_id);
    fprintf(file, "\tc_w_id\t:%lu\n", params->c_w_id);
    fprintf(file, "\tc_d_id\t:%lu\n", params->c_d_id);
    fprintf(file, "\tc_last\t:%s\n", params->c_last);
    fprintf(file, "\th_amount\t:%lf\n", params->h_amount);
  } else if (query->type == TPCC_NEW_ORDER_QUERY) {
    auto params = (tpcc_new_order_params *)(&query->params);
    fprintf(file, "\tw_id\t:%lu\n", params->w_id);
    fprintf(file, "\td_id\t:%lu\n", params->d_id);
    fprintf(file, "\tc_id\t:%lu\n", params->c_id);
    fprintf(file, "\tol_cnt\t:%lu\n", params->ol_cnt);
    fprintf(file, "\to_entry_d\t:%lu\n", params->o_entry_d);
    for (uint32_t i = 0; i < params->ol_cnt; i++) {
      fprintf(file, "\t\tol_i_id\t:%lu\n", params->items[i].ol_i_id);
      fprintf(file, "\t\tol_supply_w_id\t:%lu\n",
              params->items[i].ol_supply_w_id);
      fprintf(file, "\t\tol_quantity\t:%lu\n", params->items[i].ol_quantity);
    }
  }
}

void print_query(FILE *file, BaseQuery *query) {
  switch (query->type) {
  case YCSB_QUERY:
    print_ycsb_query(file, (ycsb_query *)query);
    break;
  case TPCC_NEW_ORDER_QUERY:
  case TPCC_PAYMENT_QUERY:
    print_tpcc_query(file, (tpcc_query *)query);
    break;
  default:
    assert(false);
  }
}

template <>
string get_table_name<ycsb_params>(uint32_t id) {
  return string("Main");
}

template <>
string get_table_name<tpcc_params>(uint32_t id) {
  switch (id) {
    case 0:
      return string("Warehouse");
    case 1:
      return string("District");
    case 2:
      return string("Customer");
    case 3:
      return string("Items");
    case 4:
      return string("Stocks");
    default:
      assert(false);
  }
}

