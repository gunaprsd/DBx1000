#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

void tpcc_query::init(uint64_t thd_id, workload * h_wl) {
	double x = (double)(rand() % 100) / 100.0;
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	// TODO
	if (x < g_perc_payment)
		gen_payment(thd_id);
	else 
		gen_new_order(thd_id);
}

RC tpcc_query::remote_qry(tpcc_query * query, TPCCRemTxnType type, int dest_id) {

	void ** data = NULL;
	int * sizes = NULL;
	int num = 0;
	int total = 2;
	RemReqType rtype = RQRY;

	switch(type) {
		case TPCC_PAYMENT0 :
			total += 4;
			break;
		case TPCC_PAYMENT1 :
			total += 8;
			break;
		case TPCC_NEWORDER0 :
			total += 5;
			break;
		case TPCC_NEWORDER1 :
			total += 1;
			break;
		case TPCC_NEWORDER2 :
			total += 8;
			break;
		default:
			assert(false);
	}
	data = new void *[total];
	sizes = new int [total];

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &type;
	sizes[num++] = sizeof(type); 
	switch(type) {
		case TPCC_PAYMENT0 :
			data[num] = &query->w_id;
			sizes[num++] = sizeof(query->w_id);
			data[num] = &query->d_id;
			sizes[num++] = sizeof(query->d_id);
			data[num] = &query->d_w_id;
			sizes[num++] = sizeof(query->d_w_id);
			data[num] = &query->h_amount;
			sizes[num++] = sizeof(query->h_amount);
			break;
		case TPCC_PAYMENT1 :
			data[num] = &query->w_id;
			sizes[num++] = sizeof(query->w_id);
			data[num] = &query->d_id;
			sizes[num++] = sizeof(query->d_id);
			data[num] = &query->c_id;
			sizes[num++] = sizeof(query->c_id);
			data[num] = &query->c_w_id;
			sizes[num++] = sizeof(query->c_w_id);
			data[num] = &query->c_d_id;
			sizes[num++] = sizeof(query->c_d_id);
			data[num] = &query->c_last;
			sizes[num++] = sizeof(query->c_last);
			data[num] = &query->h_amount;
			sizes[num++] = sizeof(query->h_amount);
			data[num] = &query->by_last_name;
			sizes[num++] = sizeof(query->by_last_name);
			break;
		case TPCC_NEWORDER0 :
			data[num] = &query->w_id;
			sizes[num++] = sizeof(query->w_id);
			data[num] = &query->d_id;
			sizes[num++] = sizeof(query->d_id);
			data[num] = &query->c_id;
			sizes[num++] = sizeof(query->c_id);
			data[num] = &query->remote;
			sizes[num++] = sizeof(query->remote);
			data[num] = &query->ol_cnt;
			sizes[num++] = sizeof(query->ol_cnt);
			break;
		case TPCC_NEWORDER1 :
			data[num] = &query->ol_i_id;
			sizes[num++] = sizeof(query->ol_i_id);
			break;
		case TPCC_NEWORDER2 :
			data[num] = &query->w_id;
			sizes[num++] = sizeof(query->w_id);
			data[num] = &query->d_id;
			sizes[num++] = sizeof(query->d_id);
			data[num] = &query->remote;
			sizes[num++] = sizeof(query->remote);
			data[num] = &query->ol_i_id;
			sizes[num++] = sizeof(query->ol_i_id);
			data[num] = &query->ol_supply_w_id;
			sizes[num++] = sizeof(query->ol_supply_w_id);
			data[num] = &query->ol_quantity;
			sizes[num++] = sizeof(query->ol_quantity);
			data[num] = &query->ol_number;
			sizes[num++] = sizeof(query->ol_number);
			data[num] = &query->o_id;
			sizes[num++] = sizeof(query->o_id);
			break;

		default:
			assert(false);
	}
	// Blocks while waiting for response
	// FIXME: Use tid as param
	RC rc;
	void * buf;
	buf = rem_qry_man.send_remote_query(dest_id, data, sizes, num, 0);
	unpack_rsp(query,buf,&rc);
	return rc;
}

void tpcc_query::remote_rsp(base_query * query, RC rc) {
	tpcc_query * m_query = (tpcc_query *) query;
	int total = 4;
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;
	RemReqType rtype = RQRY_RSP;

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &m_query->type;
	sizes[num++] = sizeof(m_query->type);
	data[num] = &rc;
	sizes[num++] = sizeof(RC);
	switch(m_query->type) {
		case TPCC_NEWORDER0 :
			data[num] = &m_query->o_id;
			sizes[num++] = sizeof(m_query->o_id);
		default:
			break;
	}
	rem_qry_man.send_remote_rsp(m_query->return_id, data, sizes, num,0);
}

void tpcc_query::unpack_rsp(base_query * query, void * d, RC * rc) {
	char * data = (char *) d;
	tpcc_query * m_query = (tpcc_query *) query;
	uint64_t ptr = HEADER_SIZE;
	memcpy(&m_query->type,&data[ptr],sizeof(m_query->type));
	ptr += sizeof(m_query->type);
	memcpy(rc,&data[ptr],sizeof(RC));
	ptr += sizeof(RC);
	switch(m_query->type) {
		case TPCC_NEWORDER0 :
			memcpy(&m_query->o_id,&data[ptr],sizeof(m_query->o_id));
			ptr += sizeof(m_query->o_id);
			break;
		default:
			break;
	}
}

void tpcc_query::unpack(base_query * query, char * data) {
	tpcc_query * m_query = (tpcc_query *) query;
	uint64_t ptr = HEADER_SIZE + sizeof(RemReqType);
	memcpy(&m_query->type,&data[ptr],sizeof(m_query->type));
	ptr += sizeof(m_query->type);
	switch(m_query->type) {
		case TPCC_PAYMENT0 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->d_w_id,&data[ptr],sizeof(m_query->d_w_id));
			ptr += sizeof(m_query->d_w_id);
			memcpy(&m_query->h_amount,&data[ptr],sizeof(m_query->h_amount));
			ptr += sizeof(m_query->h_amount);
			break;
		case TPCC_PAYMENT1 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->c_id,&data[ptr],sizeof(m_query->c_id));
			ptr += sizeof(m_query->c_id);
			memcpy(&m_query->c_w_id,&data[ptr],sizeof(m_query->c_w_id));
			ptr += sizeof(m_query->c_w_id);
			memcpy(&m_query->c_d_id,&data[ptr],sizeof(m_query->c_d_id));
			ptr += sizeof(m_query->c_d_id);
			memcpy(&m_query->c_last,&data[ptr],sizeof(m_query->c_last));
			ptr += sizeof(m_query->c_last);
			memcpy(&m_query->h_amount,&data[ptr],sizeof(m_query->h_amount));
			ptr += sizeof(m_query->h_amount);
			memcpy(&m_query->by_last_name,&data[ptr],sizeof(m_query->by_last_name));
			ptr += sizeof(m_query->by_last_name);
			break;
		case TPCC_NEWORDER0 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->c_id,&data[ptr],sizeof(m_query->c_id));
			ptr += sizeof(m_query->c_id);
			memcpy(&m_query->remote,&data[ptr],sizeof(m_query->remote));
			ptr += sizeof(m_query->remote);
			memcpy(&m_query->ol_cnt,&data[ptr],sizeof(m_query->ol_cnt));
			ptr += sizeof(m_query->ol_cnt);
			break;
		case TPCC_NEWORDER1 :
			memcpy(&m_query->ol_i_id,&data[ptr],sizeof(m_query->ol_i_id));
			ptr += sizeof(m_query->ol_i_id);
			break;
		case TPCC_NEWORDER2 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->remote,&data[ptr],sizeof(m_query->remote));
			ptr += sizeof(m_query->remote);
			memcpy(&m_query->ol_i_id,&data[ptr],sizeof(m_query->ol_i_id));
			ptr += sizeof(m_query->ol_i_id);
			memcpy(&m_query->ol_supply_w_id,&data[ptr],sizeof(m_query->ol_supply_w_id));
			ptr += sizeof(m_query->ol_supply_w_id);
			memcpy(&m_query->ol_quantity,&data[ptr],sizeof(m_query->ol_quantity));
			ptr += sizeof(m_query->ol_quantity);
			memcpy(&m_query->ol_number,&data[ptr],sizeof(m_query->ol_number));
			ptr += sizeof(m_query->ol_number);
			memcpy(&m_query->o_id,&data[ptr],sizeof(m_query->o_id));
			ptr += sizeof(m_query->o_id);
			break;
		default:
			assert(false);
	}
}
void tpcc_query::gen_payment(uint64_t thd_id) {
	type = TPCC_PAYMENT;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_w_id = w_id;
	uint64_t part_id = wh_to_part(w_id);
	part_to_access[0] = part_id;
	part_num = 1;

	d_id = URand(1, DIST_PER_WARE);
	h_amount = URand(1, 5000);
	int x = URand(1, 100);
	int y = URand(1, 100);


	if(x <= 85) { 
		// home warehouse
		c_d_id = d_id;
		c_w_id = w_id;
	} else {	
		// remote warehouse
		c_d_id = URand(1, DIST_PER_WARE);
		if(g_num_wh > 1) {
			while((c_w_id = URand(1, g_num_wh)) == w_id) {}
			if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
				part_to_access[1] = wh_to_part(c_w_id);
				part_num = 2;
			}
		} else 
			c_w_id = w_id;
	}
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist);
	}
}

void tpcc_query::gen_new_order(uint64_t thd_id) {
	type = TPCC_NEW_ORDER;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_id = URand(1, DIST_PER_WARE);
	c_id = NURand(1023, 1, g_cust_per_dist);
	rbk = URand(1, 100);
	ol_cnt = URand(5, 15);
	o_entry_d = 2013;
	items = (Item_no *) mem_allocator.alloc(sizeof(Item_no) * ol_cnt, thd_id);
	remote = false;
	part_to_access[0] = wh_to_part(w_id);
	part_num = 1;

	for (UInt32 oid = 0; oid < ol_cnt; oid ++) {
		items[oid].ol_i_id = NURand(8191, 1, g_max_items);
		UInt32 x = URand(1, 100);
		if (x > 1 || g_num_wh == 1)
			items[oid].ol_supply_w_id = w_id;
		else  {
			while((items[oid].ol_supply_w_id = URand(1, g_num_wh)) == w_id) {}
			remote = true;
		}
		items[oid].ol_quantity = URand(1, 10);
	}
	// Remove duplicate items
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		for (UInt32 j = 0; j < i; j++) {
			if (items[i].ol_i_id == items[j].ol_i_id) {
				for (UInt32 k = i; k < ol_cnt - 1; k++)
					items[k] = items[k + 1];
				ol_cnt --;
				i--;
			}
		}
	}
	for (UInt32 i = 0; i < ol_cnt; i ++) 
		for (UInt32 j = 0; j < i; j++) 
			assert(items[i].ol_i_id != items[j].ol_i_id);
	// update part_to_access
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		UInt32 j;
		for (j = 0; j < part_num; j++ ) 
			if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
				break;
		if (j == part_num) // not found! add to it.
		part_to_access[part_num ++] = wh_to_part( items[i].ol_supply_w_id );
	}
}

void 
tpcc_query::gen_order_status(uint64_t thd_id) {
	type = TPCC_ORDER_STATUS;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_id = URand(1, DIST_PER_WARE);
	c_w_id = w_id;
	c_d_id = d_id;
	int y = URand(1, 100);
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist);
	}
}

/*
void 
tpcc_query::gen_delivery(uint64_t thd_id) {
/	type = TPCC_DELIVERY;
//	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
//	else
//		w_id = URand(1, g_num_wh);
	o_carrier_id = URand(1, 10);
	ol_delivery_d = 2014;
}
*/
//uint64_t tpcc_query::wh_to_part(uint64_t wid) {
//	uint64_t part_id;
//	assert(g_part_cnt <= g_num_wh);
//	part_id = wid % g_part_cnt;
//	return part_id;
//}
