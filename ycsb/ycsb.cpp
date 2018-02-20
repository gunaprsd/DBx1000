#include "ycsb.h"

uint64_t YCSBUtility::max_key;

void YCSBUtility::initialize(YCSBBenchmarkConfig &config) {
  max_key = config.table_size;
}

template <>
void AccessIterator<ycsb_params>::set_query(Query<ycsb_params> *query) {
  _current_req_id = 0;
  _query = query;
}

template <>
bool AccessIterator<ycsb_params>::next(uint64_t &key, access_t &type, uint32_t & table_id) {
  if (_current_req_id < _query->params.request_cnt) {
    ycsb_request *request = &_query->params.requests[_current_req_id];
    key = YCSBUtility::get_hash(request->key);
    type = request->rtype;
    table_id = 0;
    _current_req_id++;
    return true;
  } else {
    return false;
  }
}

template <> uint64_t AccessIterator<ycsb_params>::get_max_key() {
  return YCSBUtility::get_max_key();
}

template <> uint32_t AccessIterator<ycsb_params>::get_num_tables() {
  return 1;
}

template <> void AccessIterator<ycsb_params>::set_cc_info(char cc_info) {
  if(_current_req_id < 0 || _current_req_id > _query->params.request_cnt) {
    assert(false);
  } else {
    _query->params.requests[_current_req_id-1].cc_info = cc_info;
  }
}
template<>
uint32_t AccessIterator<ycsb_params>::max_access_per_txn(uint32_t table_id) {
  if(table_id == 0) {
    return YCSB_NUM_REQUESTS;
  } else {
    return 0;
  }
}

template<>
void Query<ycsb_params>::obtain_rw_set(ReadWriteSet* rwset) {
  rwset->num_accesses = 0;
  for(uint32_t i = 0; i < params.request_cnt; i++) {
    rwset->add_access(0, YCSBUtility::get_hash(params.requests[i].key), params.requests[i].rtype);
  }
}