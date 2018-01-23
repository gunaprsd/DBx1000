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

typedef AccessIterator<tpcc_params> tpcc_access_iterator;

class TPCCAccessHelper {
		static uint64_t wh_cnt;
		static uint64_t wh_off;
		static uint64_t district_cnt;
		static uint64_t district_off;
		static uint64_t customer_cnt;
		static uint64_t customer_off;
		static uint64_t items_cnt;
		static uint64_t items_off;
		static uint64_t stocks_cnt;
		static uint64_t stocks_off;
public:
		static void initialize();
		static uint64_t get_max_key();
		static uint64_t get_warehouse_key(uint64_t wid);
		static uint64_t get_district_key(uint64_t wid, uint64_t did);
		static uint64_t get_customer_key(uint64_t wid, uint64_t did, uint64_t cid);
		static uint64_t get_item_key(uint64_t iid);
		static uint64_t get_stock_key(uint64_t wid, uint64_t iid);
};
#endif // TPCC_TPCC_H_
