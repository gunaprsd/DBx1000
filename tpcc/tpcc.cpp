//
// Created by Guna Prasaad on 22/01/18.
//
#include "tpcc.h"
#include "tpcc_helper.h"
uint64_t TPCCAccessHelper::wh_cnt;
uint64_t TPCCAccessHelper::wh_off;
uint64_t TPCCAccessHelper::district_cnt;
uint64_t TPCCAccessHelper::district_off;
uint64_t TPCCAccessHelper::customer_cnt;
uint64_t TPCCAccessHelper::customer_off;
uint64_t TPCCAccessHelper::items_cnt;
uint64_t TPCCAccessHelper::items_off;
uint64_t TPCCAccessHelper::stocks_cnt;
uint64_t TPCCAccessHelper::stocks_off;


uint64_t TPCCAccessHelper::get_warehouse_key(uint64_t wid) {
	return wh_off + (wid % wh_cnt);
}

uint64_t TPCCAccessHelper::get_district_key(uint64_t wid, uint64_t did) {
	return district_off + (TPCCUtility::getDistrictKey(did, wid) % district_cnt);
}

uint64_t TPCCAccessHelper::get_customer_key(uint64_t wid, uint64_t did, uint64_t cid) {
	return customer_off + (TPCCUtility::getCustomerPrimaryKey(cid, did, wid) % customer_cnt);
}

uint64_t TPCCAccessHelper::get_item_key(uint64_t iid) {
	return items_off + (iid % items_cnt);
}

uint64_t TPCCAccessHelper::get_stock_key(uint64_t wid, uint64_t iid) {
	return stocks_off + (TPCCUtility::getStockKey(iid, wid) % stocks_cnt);
}

void TPCCAccessHelper::initialize() {
	wh_cnt = g_num_wh;
	wh_off = 0;
	district_cnt = DIST_PER_WARE * wh_cnt;
	district_off = wh_off + wh_cnt;
	customer_cnt = district_cnt * g_cust_per_dist;
	customer_off = district_off + district_cnt;
	items_cnt = g_max_items;
	items_off = customer_off + customer_cnt;
	stocks_cnt = g_max_items * wh_cnt;
	stocks_off = items_off + items_cnt;
}

uint64_t TPCCAccessHelper::get_max_key() {
	return stocks_off + stocks_cnt;
}

template<>
bool AccessIterator<tpcc_params>::getNextAccess(uint64_t &key, access_t &type) {
	if(_query->type == TPCC_PAYMENT_QUERY) {
		auto payment_params = reinterpret_cast<tpcc_payment_params*>(&_query->params);
		switch (_current_req_id) {
			case 0:
				key = TPCCAccessHelper::get_warehouse_key(payment_params->w_id);
				type = g_wh_update ? WR : RD;
				break;
			case 1:
				key = TPCCAccessHelper::get_district_key(payment_params->d_w_id, payment_params->d_id);
				type = RD;
				break;
			case 2:
				if(!payment_params->by_last_name) {
					key = TPCCAccessHelper::get_customer_key(payment_params->c_w_id, payment_params->c_d_id, payment_params->c_id);
					type = WR;
				} else {
					return false;
				}
				break;
			default:
				return false;
		}
	} else if(_query->type == TPCC_NEW_ORDER_QUERY) {
		auto new_order_params = reinterpret_cast<tpcc_new_order_params*>(&_query->params);
		switch (_current_req_id) {
			case 0:
				key = TPCCAccessHelper::get_warehouse_key(new_order_params->w_id);
				type = g_wh_update ? WR : RD;
				break;
			case 1:
				key = TPCCAccessHelper::get_district_key(new_order_params->w_id, new_order_params->d_id);
				type = RD;
				break;
			case 2:
				key = TPCCAccessHelper::get_customer_key(new_order_params->w_id, new_order_params->d_id, new_order_params->c_id);
				type = WR;
				break;
			default:
				if(_current_req_id > 2 && _current_req_id < 2 * new_order_params->ol_cnt + 3) {
					int32_t index = _current_req_id - 3;
					int32_t item = index / 2;
					if(index % 2 == 0) {
						// return item
						key = TPCCAccessHelper::get_item_key(new_order_params->items[item].ol_i_id);
						type = RD;
					} else {
						// return stock
						key = TPCCAccessHelper::get_stock_key(new_order_params->items[item].ol_supply_w_id, new_order_params->items[item].ol_i_id);
						type = WR;
					}
				} else {
					return false;
				}
		}
	}
	_current_req_id++;
	return true;
}

template<>
uint64_t AccessIterator<tpcc_params>::getMaxKey() {
	return TPCCAccessHelper::get_max_key();
}

template<>
void AccessIterator<tpcc_params>::setQuery(Query<tpcc_params> *query) {
	_query = query;
	_current_req_id = 0;
}