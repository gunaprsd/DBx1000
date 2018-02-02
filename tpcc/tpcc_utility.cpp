#include "tpcc_utility.h"
#include "tpcc.h"
#include <cstring>

TPCCHelper::TPCCHelper(uint64_t num_threads) : random(num_threads) {
  C_255 = (uint64_t)generateRandom(0, 255, 0);
  C_1023 = (uint64_t)generateRandom(0, 1023, 0);
  C_8191 = (uint64_t)generateRandom(0, 8191, 0);
}

uint64_t TPCCHelper::generateRandom(uint64_t max, uint64_t thd_id) {
  return random.nextInt64(static_cast<uint32_t>(thd_id)) % max;
}

uint64_t TPCCHelper::generateRandom(uint64_t x, uint64_t y, uint64_t thd_id) {
  return x + generateRandom(y - x + 1, thd_id);
}

uint64_t TPCCHelper::generateNonUniformRandom(uint64_t A, uint64_t x,
                                              uint64_t y, uint64_t thd_id) {
  int C = 0;
  switch (A) {
  case 255:
    C = static_cast<int>(C_255);
    break;
  case 1023:
    C = static_cast<int>(C_1023);
    break;
  case 8191:
    C = static_cast<int>(C_8191);
    break;
  default:
    M_ASSERT(false, "Error! generateNonUniformRandom\n");
    exit(-1);
  }
  return (((generateRandom(0, A, thd_id) | generateRandom(x, y, thd_id)) + C) %
          (y - x + 1)) +
         x;
}

uint64_t TPCCHelper::generateAlphaString(int min, int max, char *str,
                                         uint64_t thd_id) {
  char char_list[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
                      'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                      'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
                      'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                      'U', 'V', 'W', 'X', 'Y', 'Z'};
  uint64_t cnt = generateRandom(min, max, thd_id);
  for (uint32_t i = 0; i < cnt; i++)
    str[i] = char_list[generateRandom(0L, 60L, thd_id)];
  for (int i = cnt; i < max; i++)
    str[i] = '\0';

  return cnt;
}

uint64_t TPCCHelper::generateNumberString(int min, int max, char *str,
                                          uint64_t thd_id) {

  uint64_t cnt = generateRandom(min, max, thd_id);
  for (uint32_t i = 0; i < cnt; i++) {
    uint64_t r = generateRandom(0L, 9L, thd_id);
    str[i] = '0' + r;
  }
  return cnt;
}

uint64_t TPCCUtility::findLastNameForNum(uint64_t num, char *name) {
  static const char *n[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};
  strcpy(name, n[num / 100]);
  strcat(name, n[(num / 10) % 10]);
  strcat(name, n[num % 10]);
  return strlen(name);
}

TPCCBenchmarkConfig TPCCUtility::config;
uint64_t TPCCUtility::wh_cnt;
uint64_t TPCCUtility::wh_off;
uint64_t TPCCUtility::district_cnt;
uint64_t TPCCUtility::district_off;
uint64_t TPCCUtility::customer_cnt;
uint64_t TPCCUtility::customer_off;
uint64_t TPCCUtility::items_cnt;
uint64_t TPCCUtility::items_off;
uint64_t TPCCUtility::stocks_cnt;
uint64_t TPCCUtility::stocks_off;

void TPCCUtility::initialize(const TPCCBenchmarkConfig &_config) {
  config = _config;
  wh_cnt = config.num_warehouses;
  wh_off = 0;
  district_cnt = config.districts_per_warehouse * wh_cnt;
  district_off = wh_off + wh_cnt;
  customer_cnt = district_cnt * config.customers_per_district;
  customer_off = district_off + district_cnt;
  items_cnt = config.items_count;
  items_off = customer_off + customer_cnt;
  stocks_cnt = config.items_count * wh_cnt;
  stocks_off = items_off + items_cnt;
}

uint64_t TPCCUtility::getDistrictKey(uint64_t d_id, uint64_t d_w_id) {
  return d_w_id * config.districts_per_warehouse + d_id;
}

uint64_t TPCCUtility::getCustomerPrimaryKey(uint64_t c_id, uint64_t c_d_id,
                                            uint64_t c_w_id) {
  return (getDistrictKey(c_d_id, c_w_id) * config.customers_per_district +
          c_id);
}

uint64_t TPCCUtility::getOrderLineKey(uint64_t w_id, uint64_t d_id,
                                      uint64_t o_id) {
  return getDistrictKey(d_id, w_id) * config.customers_per_district + o_id;
}

uint64_t TPCCUtility::getOrderPrimaryKey(uint64_t w_id, uint64_t d_id,
                                         uint64_t o_id) {
  return getOrderLineKey(w_id, d_id, o_id);
}

uint64_t TPCCUtility::getCustomerLastNameKey(char *c_last, uint64_t c_d_id,
                                             uint64_t c_w_id) {
  uint64_t key = 0;
  char offset = 'A';
  for (uint32_t i = 0; i < strlen(c_last); i++)
    key = (key << 2) + (c_last[i] - offset);
  key = key << 3;
  key += c_w_id * config.districts_per_warehouse + c_d_id;
  return key;
}

uint64_t TPCCUtility::getStockKey(uint64_t s_i_id, uint64_t s_w_id) {
  return s_w_id * config.items_count + s_i_id;
}

int TPCCUtility::getPartition(uint64_t wid) {
  return (int)wid % config.num_warehouses;
}

uint64_t TPCCUtility::getWarehouseHashIndex(uint64_t wid) {
  return wh_off + (wid % wh_cnt);
}

uint64_t TPCCUtility::getDistrictHashIndex(uint64_t wid, uint64_t did) {
  return district_off + (TPCCUtility::getDistrictKey(did, wid) % district_cnt);
}

uint64_t TPCCUtility::getCustomerHashIndex(uint64_t wid, uint64_t did,
                                           uint64_t cid) {
  return customer_off +
         (TPCCUtility::getCustomerPrimaryKey(cid, did, wid) % customer_cnt);
}

uint64_t TPCCUtility::getItemsHashIndex(uint64_t iid) {
  return items_off + (iid % items_cnt);
}

uint64_t TPCCUtility::getStocksHashIndex(uint64_t wid, uint64_t iid) {
  return stocks_off + (TPCCUtility::getStockKey(iid, wid) % stocks_cnt);
}

uint64_t TPCCUtility::getHashSize() { return stocks_off + stocks_cnt; }

template <>
bool AccessIterator<tpcc_params>::next(uint64_t &key, access_t &type, uint32_t & table_id) {
  if (_query->type == TPCC_PAYMENT_QUERY) {
    auto payment_params =
        reinterpret_cast<tpcc_payment_params *>(&_query->params);
    switch (_current_req_id) {
    case 0:
      key = TPCCUtility::getWarehouseHashIndex(payment_params->w_id);
      type = TPCCUtility::config.warehouse_update ? WR : RD;
			table_id = 0;
      break;
    case 1:
      key = TPCCUtility::getDistrictHashIndex(payment_params->d_w_id,
                                              payment_params->d_id);
      type = RD;
				table_id = 1;
      break;
    case 2:
      if (!payment_params->by_last_name) {
        key = TPCCUtility::getCustomerHashIndex(payment_params->c_w_id,
                                                payment_params->c_d_id,
                                                payment_params->c_id);
        type = WR;
				table_id = 2;
      } else {
        return false;
      }
      break;
    default:
      return false;
    }
  } else if (_query->type == TPCC_NEW_ORDER_QUERY) {
    auto new_order_params =
        reinterpret_cast<tpcc_new_order_params *>(&_query->params);
    switch (_current_req_id) {
    case 0:
      key = TPCCUtility::getWarehouseHashIndex(new_order_params->w_id);
      type = RD;
			table_id = 0;
      break;
    case 1:
      key = TPCCUtility::getDistrictHashIndex(new_order_params->w_id,
                                              new_order_params->d_id);
      type = RD;
				table_id = 1;
      break;
    case 2:
      key = TPCCUtility::getCustomerHashIndex(new_order_params->w_id,
                                              new_order_params->d_id,
                                              new_order_params->c_id);
      type = WR;
				table_id = 2;
      break;
    default:
      if (_current_req_id > 2 &&
          _current_req_id < 2 * new_order_params->ol_cnt + 3) {
        int32_t index = _current_req_id - 3;
        int32_t item = index / 2;
        if (index % 2 == 0) {
          // return item
          key = TPCCUtility::getItemsHashIndex(
              new_order_params->items[item].ol_i_id);
					table_id = 3;
          type = RD;
        } else {
          // return stock
          key = TPCCUtility::getStocksHashIndex(
              new_order_params->items[item].ol_supply_w_id,
              new_order_params->items[item].ol_i_id);
          type = WR;
					table_id = 4;
        }
      } else {
        return false;
      }
    }
  }
  _current_req_id++;
  return true;
}

template <> uint64_t AccessIterator<tpcc_params>::get_max_key() {
  return TPCCUtility::getHashSize();
}

template<> uint32_t AccessIterator<tpcc_params>::get_num_tables() {
	return 5;
}
template <>
void AccessIterator<tpcc_params>::set_query(Query<tpcc_params> *query) {
  _query = query;
  _current_req_id = 0;
}

template <>
void AccessIterator<tpcc_params>::set_cc_info(char cc_info) {
  assert(false);
}
