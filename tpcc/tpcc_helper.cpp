#include "tpcc_helper.h"

TPCCUtility::TPCCUtility(uint32_t num_threads) : random(num_threads) {
  C_255 = (uint64_t)generateRandom(0, 255, 0);
  C_1023 = (uint64_t)generateRandom(0, 1023, 0);
  C_8191 = (uint64_t)generateRandom(0, 8191, 0);
}

uint64_t TPCCUtility::generateRandom(uint64_t max, uint64_t thd_id) {
  return random.nextInt64(static_cast<uint32_t>(thd_id)) % max;
}

uint64_t TPCCUtility::generateRandom(uint64_t x, uint64_t y, uint64_t thd_id) {
  return x + generateRandom(y - x + 1, thd_id);
}

uint64_t TPCCUtility::generateNonUniformRandom(uint64_t A, uint64_t x,
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

uint64_t TPCCUtility::generateAlphaString(int min, int max, char *str,
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

uint64_t TPCCUtility::generateNumberString(int min, int max, char *str,
                                           uint64_t thd_id) {

  uint64_t cnt = generateRandom(min, max, thd_id);
  for (UInt32 i = 0; i < cnt; i++) {
    uint64_t r = generateRandom(0L, 9L, thd_id);
    str[i] = '0' + r;
  }
  return cnt;
}

uint64_t TPCCUtility::getDistrictKey(uint64_t d_id, uint64_t d_w_id) {
  return d_w_id * DIST_PER_WARE + d_id;
}

uint64_t TPCCUtility::getCustomerPrimaryKey(uint64_t c_id, uint64_t c_d_id,
                                            uint64_t c_w_id) {
  return (getDistrictKey(c_d_id, c_w_id) * g_cust_per_dist + c_id);
}

uint64_t TPCCUtility::getOrderLineKey(uint64_t w_id, uint64_t d_id,
                                      uint64_t o_id) {
  return getDistrictKey(d_id, w_id) * g_cust_per_dist + o_id;
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
  key += c_w_id * DIST_PER_WARE + c_d_id;
  return key;
}

uint64_t TPCCUtility::getStockKey(uint64_t s_i_id, uint64_t s_w_id) {
  return s_w_id * g_max_items + s_i_id;
}

uint64_t TPCCUtility::findLastNameForNum(uint64_t num, char *name) {
  static const char *n[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};
  strcpy(name, n[num / 100]);
  strcat(name, n[(num / 10) % 10]);
  strcat(name, n[num % 10]);
  return strlen(name);
}

int TPCCUtility::getPartition(uint64_t wid) {
  assert(g_part_cnt <= g_num_wh);
  return (int)wid % g_part_cnt;
}
