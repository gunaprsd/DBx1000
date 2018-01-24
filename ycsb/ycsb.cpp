#include "ycsb.h"

uint64_t YCSBUtility::max_key;

void YCSBUtility::initialize(YCSBBenchmarkConfig & config) {
	max_key = config.table_size;
}

template<>
void AccessIterator<ycsb_params>::set_query(Query<ycsb_params> *query)  {
	_current_req_id = 0;
	_query = query;
}

template<>
bool AccessIterator<ycsb_params>::next(uint64_t &key, access_t &type)  {
	if(_current_req_id < _query->params.request_cnt) {
		ycsb_request* request = &_query->params.requests[_current_req_id];
		key = YCSBUtility::get_hash(request->key);
		type = request->rtype;
		_current_req_id++;
		return true;
	} else {
		return false;
	}
}

template<>
uint64_t AccessIterator<ycsb_params>::get_max_key() {
	return YCSBUtility::get_max_key();
}