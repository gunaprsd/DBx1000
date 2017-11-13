#ifndef ROW_OCC_H
#define ROW_OCC_H

class Table;
class Catalog;
class TransactionManager;
struct TsReqEntry;

class Row_occ {
public:
	void 				init(Row * row);
	Status 					access(TransactionManager * txn, TimestampType type);
	void 				latch();
	// ts is the start_ts of the validating txn 
	bool				validate(uint64_t ts);
	void				write(Row * data, uint64_t ts);
	void 				release();
private:
 	pthread_mutex_t * 	_latch;
	bool 				blatch;

	Row * 			_row;
	// the last update time
	Time 				wts;
};

#endif
