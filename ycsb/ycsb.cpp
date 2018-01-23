#include "ycsb.h"

template<>
void AccessIterator<ycsb_params>::setQuery(Query<ycsb_params>* query)  {
	_current_req_id = 0;
	_query = query;
}

template<>
bool AccessIterator<ycsb_params>::getNextAccess(uint64_t & key, access_t & type)  {
	if(_current_req_id < _query->params.request_cnt) {
		ycsb_request* request = &_query->params.requests[_current_req_id];
		key = YCSBAccessHelper::get_hash(request->key);
		type = request->rtype;
		_current_req_id++;
		return true;
	} else {
		return false;
	}
}

template<>
uint64_t AccessIterator<ycsb_params>::getMaxKey() {
	return YCSBAccessHelper::get_max_key();
}