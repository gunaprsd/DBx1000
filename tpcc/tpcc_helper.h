#ifndef TPCC_TPCC_HELPER_H_
#define TPCC_TPCC_HELPER_H_

#include "global.h"
#include "helper.h"
#include "distributions.h"


class TPCCUtility {
		uint64_t C_255, C_1023, C_8191;
public:
		RandomNumberGenerator random;
		TPCCUtility(uint32_t num_threads);
		uint64_t generateRandom(uint64_t max, uint64_t thd_id);
		uint64_t generateRandom(uint64_t x, uint64_t y, uint64_t thd_id);
		uint64_t generateNonUniformRandom(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id);
		uint64_t generateAlphaString(int min, int max, char *str, uint64_t thd_id);
		uint64_t generateNumberString(int min, int max, char *str, uint64_t thd_id);

		static uint64_t getDistrictKey(uint64_t d_id, uint64_t d_w_id);
		static uint64_t getCustomerPrimaryKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);
		static uint64_t getCustomerLastNameKey(char *c_last, uint64_t c_d_id, uint64_t c_w_id);
		static uint64_t getOrderLineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
		static uint64_t getOrderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
		static uint64_t getStockKey(uint64_t s_i_id, uint64_t s_w_id);
		static uint64_t findLastNameForNum(uint64_t num, char *name);
		static int getPartition(uint64_t wid);
};


#define COPY_CONST_STRING(dst, src, len) memcpy(dst, src, len); dst[len] = '\0';

#endif
