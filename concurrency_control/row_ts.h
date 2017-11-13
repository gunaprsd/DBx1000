#ifndef ROW_TS_H
#define ROW_TS_H

class Table;
class Catalog;
class TransactionManager;
struct TsReqEntry {
	TransactionManager * txn;
	// for write requests, need to have a copy of the data to write.
	Row * row;
	Record * item;
	Time ts;
	TsReqEntry * next;
};

class Row_ts {
public:
	void init(Row * row);
	Status access(TransactionManager * txn, TimestampType type, Row * row);

private:
 	pthread_mutex_t * latch;
	bool blatch;

	void buffer_req(TimestampType type, TransactionManager * txn, Row * row);
	TsReqEntry * debuffer_req(TimestampType type, TransactionManager * txn);
	TsReqEntry * debuffer_req(TimestampType type, Time ts);
	TsReqEntry * debuffer_req(TimestampType type, TransactionManager * txn, Time ts);
	void update_buffer();
	Time cal_min(TimestampType type);
	TsReqEntry * get_req_entry();
	void return_req_entry(TsReqEntry * entry);
 	void return_req_list(TsReqEntry * list);

	Row * _row;
	Time wts;
    Time rts;
    Time min_wts;
    Time min_rts;
    Time min_pts;

    TsReqEntry * readreq;
    TsReqEntry * writereq;
    TsReqEntry * prereq;
	uint64_t preq_len;
};

#endif
