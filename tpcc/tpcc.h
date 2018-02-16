// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_H_
#define TPCC_TPCC_H_

#include "query.h"
#include "tpcc_const.h"
#define MAX_ACCESS_IN_PAYMENT 3
#define MAX_ACCESS_IN_NEW_ORDER 3 + 2 * TPCC_MAX_NUM_ORDERS

struct ol_item {
  uint64_t ol_i_id;
  uint64_t ol_supply_w_id;
  uint64_t ol_quantity;
};

struct tpcc_payment_params {
	char cc_info[MAX_ACCESS_IN_PAYMENT];
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
	char cc_info[MAX_ACCESS_IN_NEW_ORDER];
	uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;
  ol_item items[TPCC_MAX_NUM_ORDERS];
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

struct TPCCBenchmarkConfig {
		uint32_t num_warehouses;
		uint64_t items_count;
		uint64_t districts_per_warehouse;
		uint64_t customers_per_district;
		double percent_payment;
		bool warehouse_update;
};



#endif // TPCC_TPCC_H_
