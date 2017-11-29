#include "tpcc.h"
#include "row.h"

void TPCCTransactionManager::initialize(Database *database, uint32_t thread_id) {
    txn_man::initialize(database, thread_id);
    db = (TPCCDatabase *) database;
}

RC TPCCTransactionManager::run_txn(BaseQuery *query) {
    auto tpcc_query = ((tpcc_query *) query);
    switch (query->type) {
        case TPCC_PAYMENT_QUERY:
            auto payment_params = ((tpcc_payment_params *) (& tpcc_query->params));
            return run_payment(payment_params);
        case TPCC_NEW_ORDER_QUERY:
            auto new_order_params = ((tpcc_new_order_params *) (& tpcc_query->params));
            return run_new_order(new_order_params);
        default:
            printf("Transaction type not supported!");
            exit(0);
    }
}

RC TPCCTransactionManager::run_payment(tpcc_payment_params * query) {
    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;

    uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;
    /*====================================================+
        EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
        WHERE w_id=:w_id;
    +====================================================*/
    /*===================================================================+
        EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
        INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
        FROM warehouse
        WHERE w_id=:w_id;
    +===================================================================*/

    key = query->w_id;
    INDEX * index = db->i_warehouse;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    if (g_wh_update)
        r_wh_local = get_row(r_wh, WR);
    else
        r_wh_local = get_row(r_wh, RD);

    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    double w_ytd;

    r_wh_local->get_value(W_YTD, w_ytd);
    if (g_wh_update) {
        r_wh_local->set_value(W_YTD, w_ytd + query->h_amount);
    }
    char w_name[11];
    char * tmp_str = r_wh_local->get_value(W_NAME);
    memcpy(w_name, tmp_str, 10);
    w_name[10] = '\0';
    /*=====================================================+
        EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +=====================================================*/
    key = distKey(query->d_id, query->d_w_id);
    item = index_read(db->i_district, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_dist = ((row_t *)item->location);
    row_t * r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }

    double d_ytd;
    r_dist_local->get_value(D_YTD, d_ytd);
    r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
    char d_name[11];
    tmp_str = r_dist_local->get_value(D_NAME);
    memcpy(d_name, tmp_str, 10);
    d_name[10] = '\0';

    /*====================================================================+
        EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
        INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
        FROM district
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +====================================================================*/

    row_t * r_cust;
    if (query->by_last_name) {
        /*==========================================================+
            EXEC SQL SELECT count(c_id) INTO :namecnt
            FROM customer
            WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
        +==========================================================*/
        /*==========================================================================+
            EXEC SQL DECLARE c_byname CURSOR FOR
            SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
            c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
            ORDER BY c_first;
            EXEC SQL OPEN c_byname;
        +===========================================================================*/

        uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
        // XXX: the list is not sorted. But let's assume it's sorted...
        // The performance won't be much different.
        INDEX * index = db->i_customer_last;
        item = index_read(index, key, wh_to_part(c_w_id));
        assert(item != NULL);

        int cnt = 0;
        itemid_t * it = item;
        itemid_t * mid = item;
        while (it != NULL) {
            cnt ++;
            it = it->next;
            if (cnt % 2 == 0)
                mid = mid->next;
        }
        r_cust = ((row_t *)mid->location);

        /*============================================================================+
            for (n=0; n<namecnt/2; n++) {
                EXEC SQL FETCH c_byname
                INTO :c_first, :c_middle, :c_id,
                     :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
                     :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
            }
            EXEC SQL CLOSE c_byname;
        +=============================================================================*/
        // XXX: we don't retrieve all the info, just the tuple we are interested in
    }
    else { // search customers by cust_id
        /*=====================================================================+
            EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
            c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
            c_discount, c_balance, c_since
            INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
            :c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
            :c_discount, :c_balance, :c_since
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
        +======================================================================*/
        key = custKey(query->c_id, query->c_d_id, query->c_w_id);
        INDEX * index = db->i_customer_id;
        item = index_read(index, key, wh_to_part(c_w_id));
        assert(item != NULL);
        r_cust = (row_t *) item->location;
    }

    /*======================================================================+
         EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
         WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
     +======================================================================*/
    row_t * r_cust_local = get_row(r_cust, WR);
    if (r_cust_local == NULL) {
        return finish(Abort);
    }
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;

    r_cust_local->get_value(C_BALANCE, c_balance);
    r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
    r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
    r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
    r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
    r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

    char * c_credit = r_cust_local->get_value(C_CREDIT);

    if ( strstr(c_credit, "BC") ) {

        /*=====================================================+
            EXEC SQL SELECT c_data
            INTO :c_data
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
        +=====================================================*/
//	  	char c_new_data[501];
//	  	sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f",
//	      	c_id, c_d_id, c_w_id, d_id, w_id, query->h_amount);
//		char * c_data = r_cust->get_value("C_DATA");
//	  	strncat(c_new_data, c_data, 500 - strlen(c_new_data));
//		r_cust->set_value("C_DATA", c_new_data);

    }

    char h_data[25];
    strncpy(h_data, w_name, 10);
    int length = strlen(h_data);
    if (length > 10) length = 10;
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length+14] = '\0';
    /*=============================================================================+
      EXEC SQL INSERT INTO
      history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
      VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
      +=============================================================================*/
//	row_t * r_hist;
//	uint64_t row_id;
//	_wl->t_history->get_new_row(r_hist, 0, row_id);
//	r_hist->set_value(H_C_ID, c_id);
//	r_hist->set_value(H_C_D_ID, c_d_id);
//	r_hist->set_value(H_C_W_ID, c_w_id);
//	r_hist->set_value(H_D_ID, d_id);
//	r_hist->set_value(H_W_ID, w_id);
//	int64_t date = 2013;
//	r_hist->set_value(H_DATE, date);
//	r_hist->set_value(H_AMOUNT, h_amount);
#if !TPCC_SMALL
//	r_hist->set_value(H_DATA, h_data);
#endif
//	insert_row(r_hist, _wl->t_history);

    assert( rc == RCOK );
    return finish(rc);
}

RC TPCCTransactionManager::run_new_order(tpcc_new_order_params * query) {
    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;
    INDEX * index;

    bool remote = query->remote;
    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;
    /*=======================================================================+
    EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
        INTO :c_discount, :c_last, :c_credit, :w_tax
        FROM customer, warehouse
        WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
    +========================================================================*/
    key = w_id;
    index = db->i_warehouse;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local = get_row(r_wh, RD);
    if (r_wh_local == NULL) {
        return finish(Abort);
    }


    double w_tax;
    r_wh_local->get_value(W_TAX, w_tax);
    key = custKey(c_id, d_id, w_id);
    index = db->i_customer_id;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_cust = (row_t *) item->location;
    row_t * r_cust_local = get_row(r_cust, RD);
    if (r_cust_local == NULL) {
        return finish(Abort);
    }
    uint64_t c_discount;
    //char * c_last;
    //char * c_credit;
    r_cust_local->get_value(C_DISCOUNT, c_discount);
    //c_last = r_cust_local->get_value(C_LAST);
    //c_credit = r_cust_local->get_value(C_CREDIT);

    /*==================================================+
    EXEC SQL SELECT d_next_o_id, d_tax
        INTO :d_next_o_id, :d_tax
        FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
    EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
        WH ERE d _id = :d_id AN D d _w _id = :w _id ;
    +===================================================*/
    key = distKey(d_id, w_id);
    item = index_read(db->i_district, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_dist = ((row_t *)item->location);
    row_t * r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }
    //double d_tax;
    int64_t o_id;
    //d_tax = *(double *) r_dist_local->get_value(D_TAX);
    o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
    o_id ++;
    r_dist_local->set_value(D_NEXT_O_ID, o_id);

    /*========================================================================================+
    EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
        VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
    +========================================================================================*/
//	row_t * r_order;
//	uint64_t row_id;
//	_wl->t_order->get_new_row(r_order, 0, row_id);
//	r_order->set_value(O_ID, o_id);
//	r_order->set_value(O_C_ID, c_id);
//	r_order->set_value(O_D_ID, d_id);
//	r_order->set_value(O_W_ID, w_id);
//	r_order->set_value(O_ENTRY_D, o_entry_d);
//	r_order->set_value(O_OL_CNT, ol_cnt);
//	int64_t all_local = (remote? 0 : 1);
//	r_order->set_value(O_ALL_LOCAL, all_local);
//	insert_row(r_order, _wl->t_order);
    /*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
//	row_t * r_no;
//	_wl->t_neworder->get_new_row(r_no, 0, row_id);
//	r_no->set_value(NO_O_ID, o_id);
//	r_no->set_value(NO_D_ID, d_id);
//	r_no->set_value(NO_W_ID, w_id);
//	insert_row(r_no, _wl->t_neworder);
    for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {

        uint64_t ol_i_id = query->items[ol_number].ol_i_id;
        uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        uint64_t ol_quantity = query->items[ol_number].ol_quantity;
        /*===========================================+
        EXEC SQL SELECT i_price, i_name , i_data
            INTO :i_price, :i_name, :i_data
            FROM item
            WHERE i_id = :ol_i_id;
        +===========================================*/
        key = ol_i_id;
        item = index_read(db->i_item, key, 0);
        assert(item != NULL);
        row_t * r_item = ((row_t *)item->location);

        row_t * r_item_local = get_row(r_item, RD);
        if (r_item_local == NULL) {
            return finish(Abort);
        }
        int64_t i_price;
        //char * i_name;
        //char * i_data;
        r_item_local->get_value(I_PRICE, i_price);
        //i_name = r_item_local->get_value(I_NAME);
        //i_data = r_item_local->get_value(I_DATA);

        /*===================================================================+
        EXEC SQL SELECT s_quantity, s_data,
                s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
                s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
            INTO :s_quantity, :s_data,
                :s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
                :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
            FROM stock
            WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
        EXEC SQL UPDATE stock SET s_quantity = :s_quantity
            WHERE s_i_id = :ol_i_id
            AND s_w_id = :ol_supply_w_id;
        +===============================================*/

        uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
        INDEX * stock_index = db->i_stock;
        itemid_t * stock_item;
        index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
        assert(item != NULL);
        row_t * r_stock = ((row_t *)stock_item->location);
        row_t * r_stock_local = get_row(r_stock, WR);
        if (r_stock_local == NULL) {
            return finish(Abort);
        }

        // XXX s_dist_xx are not retrieved.
        UInt64 s_quantity;
        int64_t s_remote_cnt;
        s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
        int64_t s_ytd;
        int64_t s_order_cnt;
        //char * s_data = "test";
        r_stock_local->get_value(S_YTD, s_ytd);
        r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
        r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
        r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
        //s_data = r_stock_local->get_value(S_DATA);
#endif
        if (remote) {
            s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
            s_remote_cnt ++;
            r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
        }
        uint64_t quantity;
        if (s_quantity > ol_quantity + 10) {
            quantity = s_quantity - ol_quantity;
        } else {
            quantity = s_quantity - ol_quantity + 91;
        }
        r_stock_local->set_value(S_QUANTITY, &quantity);

        /*====================================================+
        EXEC SQL INSERT
            INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
                ol_i_id, ol_supply_w_id,
                ol_quantity, ol_amount, ol_dist_info)
            VALUES(:o_id, :d_id, :w_id, :ol_number,
                :ol_i_id, :ol_supply_w_id,
                :ol_quantity, :ol_amount, :ol_dist_info);
        +====================================================*/
        // XXX district info is not inserted.
//		row_t * r_ol;
//		uint64_t row_id;
//		_wl->t_orderline->get_new_row(r_ol, 0, row_id);
//		r_ol->set_value(OL_O_ID, &o_id);
//		r_ol->set_value(OL_D_ID, &d_id);
//		r_ol->set_value(OL_W_ID, &w_id);
//		r_ol->set_value(OL_NUMBER, &ol_number);
//		r_ol->set_value(OL_I_ID, &ol_i_id);
#if !TPCC_SMALL
//		int w_tax=1, d_tax=1;
//		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
//		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
//		r_ol->set_value(OL_QUANTITY, &ol_quantity);
//		r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif
//		insert_row(r_ol, _wl->t_orderline);
    }
    assert( rc == RCOK );
    return finish(rc);
}
