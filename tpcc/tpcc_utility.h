#ifndef TPCC_TPCC_HELPER_H_
#define TPCC_TPCC_HELPER_H_

#include "distributions.h"
#include "global.h"
#include "helper.h"
#include "tpcc.h"

class TPCCHelper {
  uint64_t C_255, C_1023, C_8191;

public:
  RandomNumberGenerator random;
  TPCCHelper(uint64_t num_threads);
  uint64_t generateRandom(uint64_t max, uint64_t thd_id);
  uint64_t generateRandom(uint64_t x, uint64_t y, uint64_t thd_id);
  uint64_t generateNonUniformRandom(uint64_t A, uint64_t x, uint64_t y,
                                    uint64_t thd_id);
  uint64_t generateAlphaString(int min, int max, char *str, uint64_t thd_id);
  uint64_t generateNumberString(int min, int max, char *str, uint64_t thd_id);
};

class TPCCUtility {
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
  static TPCCBenchmarkConfig config;
  // Static functions used by database and transactions
  static void initialize(const TPCCBenchmarkConfig &config);
  static uint64_t getDistrictKey(uint64_t d_id, uint64_t d_w_id);
  static uint64_t getCustomerPrimaryKey(uint64_t c_id, uint64_t c_d_id,
                                        uint64_t c_w_id);
  static uint64_t getCustomerLastNameKey(char *c_last, uint64_t c_d_id,
                                         uint64_t c_w_id);
  static uint64_t getOrderLineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
  static uint64_t getOrderPrimaryKey(uint64_t w_id, uint64_t d_id,
                                     uint64_t o_id);
  static uint64_t getStockKey(uint64_t s_i_id, uint64_t s_w_id);
  static uint64_t findLastNameForNum(uint64_t num, char *name);
  static int getPartition(uint64_t wid);

  // Static functions used by AccessIterator
  static uint64_t getHashSize();
  static uint64_t getWarehouseHashIndex(uint64_t wid);
  static uint64_t getDistrictHashIndex(uint64_t wid, uint64_t did);
  static uint64_t getCustomerHashIndex(uint64_t wid, uint64_t did,
                                       uint64_t cid);
  static uint64_t getItemsHashIndex(uint64_t iid);
  static uint64_t getStocksHashIndex(uint64_t wid, uint64_t iid);
};

#define COPY_CONST_STRING(dst, src, len)                                       \
  memcpy(dst, src, len);                                                       \
  dst[len] = '\0';

#endif
