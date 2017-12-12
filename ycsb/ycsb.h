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

#endif // YCSB_YCSB_H_
