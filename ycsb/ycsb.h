// Copyright[2017] <Guna Prasaad>

#ifndef YCSB_YCSB_H_
#define YCSB_YCSB_H_

#include "query.h"

class ycsb_request {
public:
  access_t rtype;
  uint64_t key;
  char value;
  uint32_t scan_len;
};

struct ycsb_params {
  uint64_t request_cnt;
  ycsb_request requests[MAX_REQ_PER_QUERY];
};

typedef Query<ycsb_params> ycsb_query;

class YCSBAccessHelper {
public:
		static uint64_t get_max_key() {
			return max_key;
		}
		static uint64_t get_hash(uint64_t key) {
			return key % max_key;
		}
protected:
		static uint64_t max_key;
};

typedef AccessIterator<ycsb_params> ycsb_access_iterator;

#endif // YCSB_YCSB_H_
