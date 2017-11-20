#ifndef _TPCC_H_
#define _TPCC_H_

#include "TransactionManager.h"
#include "Workload.h"

class Table;
class INDEX;
class TPCCQuery;

class tpcc_wl : public Workload {
public:
	Status initialize();
	Status initialize_table();
	Status init_schema(const char * schema_file);
	Status get_txn_manager(TransactionManager *& txn_manager, Thread * h_thd);
	Table * 		t_warehouse;
	Table * 		t_district;
	Table * 		t_customer;
	Table *		t_history;
	Table *		t_neworder;
	Table *		t_order;
	Table *		t_orderline;
	Table *		t_item;
	Table *		t_stock;

	INDEX * 	i_item;
	INDEX * 	i_warehouse;
	INDEX * 	i_district;
	INDEX * 	i_customer_id;
	INDEX * 	i_customer_last;
	INDEX * 	i_stock;
	INDEX * 	i_order; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline_wd; // key = (w_id, d_id). 
	
	bool ** delivering;
	uint32_t next_tid;
private:
	uint64_t num_wh;
	void init_tab_item();
	void init_tab_wh(uint32_t wid);
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(uint64_t w_id);
	void init_tab_cust(uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(uint64_t d_id, uint64_t w_id);
	
	void init_permutation(uint64_t * perm_c_id, uint64_t wid);

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);

	static void * threadInitWarehouse(void * This);
};

class tpcc_txn_man : public TransactionManager
{
public:
	void initialize(Thread * h_thd, Workload * h_wl, uint64_t part_id); 
	Status execute(Query * query);
private:
	tpcc_wl * _wl;
	Status run_payment(TPCCQuery * m_query);
	Status run_new_order(TPCCQuery * m_query);
	Status run_order_status(TPCCQuery * query);
	Status run_delivery(TPCCQuery * query);
	Status run_stock_level(TPCCQuery * query);
};

#endif
