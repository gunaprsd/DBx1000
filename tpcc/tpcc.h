// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_H_
#define TPCC_TPCC_H_

#include "query.h"
#include "tpcc_const.h"

struct ol_item {
  uint64_t ol_i_id;
  uint64_t ol_supply_w_id;
  uint64_t ol_quantity;
};

struct tpcc_payment_params {
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;
  uint64_t d_w_id;
  uint64_t c_w_id;
  uint64_t c_d_id;
  char c_last[LASTNAME_LEN];
  double h_amount;
  bool by_last_name;
};

struct tpcc_new_order_params {
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;
  ol_item items[MAX_NUM_ORDER_LINE];
  bool rbk;
  bool remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;
};

union tpcc_params {
  tpcc_new_order_params new_order_params;
  tpcc_payment_params payment_params;
};

typedef Query<tpcc_params> tpcc_query;

#endif // TPCC_TPCC_H_
