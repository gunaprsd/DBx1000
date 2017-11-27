#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "database.h"
#include "txn.h"
#include "global.h"
#include "helper.h"
#include "query.h"
#include "ycsb_database.h"

class ycsb_request {
public:
	access_t rtype;
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

struct ycsb_params {
	uint64_t 		request_cnt;
	ycsb_request 	requests[MAX_REQ_PER_QUERY];
};

typedef Query<ycsb_params> ycsb_query;


class YCSBTransactionManager : public txn_man
{
public:
	void initialize(Database * database, INDEX * index, uint32_t thread_id);
	RC run_txn(BaseQuery * query) override;

	uint64_t 		row_cnt;
	YCSBDatabase * 	ycsb_database;
	INDEX *			the_index;
};

#endif
