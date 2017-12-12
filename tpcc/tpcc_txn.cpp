#include "tpcc.h"
#include "row.h"

void TPCCTransactionManager::initialize(Database *database, uint32_t thread_id) {
    txn_man::initialize(database, thread_id);
    db = (TPCCDatabase *) database;
}

RC TPCCTransactionManager::run_txn(BaseQuery *query) {
    tpcc_query * t_query = ((tpcc_query *) query);
    tpcc_payment_params * payment_params = NULL;
    tpcc_new_order_params * new_order_params = NULL;
    switch (query->type) {
        case TPCC_PAYMENT_QUERY:
	    payment_params = ((tpcc_payment_params *) (& t_query->params));
            return run_payment(payment_params);
        case TPCC_NEW_ORDER_QUERY:
            new_order_params = ((tpcc_new_order_params *) (& t_query->params));
            return run_new_order(new_order_params);
        default:
            printf("Transaction type not supported!");
            exit(0);
    }
}

RC TPCCTransactionManager::run_payment(tpcc_payment_params * params) {
    RC rc           = RCOK;
    itemid_t * item = nullptr;
    uint64_t key    = 0;


    uint64_t w_id = params->w_id;
    uint64_t c_w_id = params->c_w_id;

    //Obtain local copy of warehouse row with appropriate access
    item = index_read(db->i_warehouse, params->w_id, wh_to_part(params->w_id));
    assert(item != nullptr);
    row_t * r_wh        = ((row_t *)item->location);
    row_t * r_wh_local  = nullptr;
    if (g_wh_update) {
        r_wh_local = get_row(r_wh, WR);
    } else {
        r_wh_local = get_row(r_wh, RD);
    }
    if (r_wh_local == nullptr) {
        return finish(Abort);
    }


    /*====================================================+
        EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
        WHERE w_id=:w_id;
    +====================================================*/

    if(g_wh_update) {
        double w_ytd;
        r_wh_local->get_value(W_YTD, w_ytd);
        r_wh_local->set_value(W_YTD, w_ytd + params->h_amount);
    }

    /*===================================================================+
        EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
        INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
        FROM warehouse
        WHERE w_id=:w_id;
    +===================================================================*/

    char w_name[11];
    char w_street_1[21];
    char w_street_2[21];
    char w_city[21];
    char w_state[3];
    char w_zip[10];

    COPY_CONST_STRING(w_name, r_wh_local->get_value(W_NAME), 10);
    COPY_CONST_STRING(w_street_1, r_wh_local->get_value(W_STREET_1), 20);
    COPY_CONST_STRING(w_street_2, r_wh_local->get_value(W_STREET_2), 20);
    COPY_CONST_STRING(w_city, r_wh_local->get_value(W_CITY), 20);
    COPY_CONST_STRING(w_state, r_wh_local->get_value(W_STATE), 2);
    COPY_CONST_STRING(w_zip, r_wh_local->get_value(W_ZIP), 9);



    key     = distKey(params->d_id, params->d_w_id);
    item    = index_read(db->i_district, key, wh_to_part(w_id));
    assert(item != nullptr);
    row_t * r_dist       = ((row_t *)item->location);
    row_t * r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == nullptr) {
        return finish(Abort);
    }

    /*=====================================================+
        EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +=====================================================*/

    double d_ytd;
    r_dist_local->get_value(D_YTD, d_ytd);
    r_dist_local->set_value(D_YTD, d_ytd + params->h_amount);

    /*====================================================================+
        EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
        INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
        FROM district
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +====================================================================*/

    char d_name[11];
    char d_street_1[21];
    char d_street_2[21];
    char d_city[21];
    char d_state[3];
    char d_zip[10];

    COPY_CONST_STRING(d_name, r_dist_local->get_value(D_NAME), 10);
    COPY_CONST_STRING(d_street_1, r_dist_local->get_value(D_STREET_1), 20);
    COPY_CONST_STRING(d_street_2, r_dist_local->get_value(D_STREET_2), 20);
    COPY_CONST_STRING(d_city, r_dist_local->get_value(D_CITY), 20);
    COPY_CONST_STRING(d_state, r_dist_local->get_value(D_STATE), 2);
    COPY_CONST_STRING(d_zip, r_dist_local->get_value(D_ZIP), 9);


    row_t * r_customer;
    if (params->by_last_name) {
        /*=======================================================================*
            EXEC SQL SELECT count(c_id) INTO :name_cnt
            FROM customer
            WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
        +========================================================================*/

        /*==========================================================================+
            EXEC SQL DECLARE c_by_name CURSOR FOR
            SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
            c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
            ORDER BY c_first;
            EXEC SQL OPEN c_by_name;
        +===========================================================================*/

        key = custNPKey(params->c_last, params->c_d_id, params->c_w_id);
        item = index_read(db->i_customer_last, key, wh_to_part(c_w_id));
        assert(item != nullptr);
        int cnt = 0;
        itemid_t * it = item;
        itemid_t * mid = item;
        while (it != nullptr) {
            cnt ++;
            it = it->next;
            if (cnt % 2 == 0) {
                mid = mid->next;
            }
        }
        r_customer = ((row_t *)mid->location);
    }
    else {
        key = custKey(params->c_id, params->c_d_id, params->c_w_id);
        item = index_read(db->i_customer_id, key, wh_to_part(c_w_id));
        assert(item != NULL);
        r_customer = (row_t *) item->location;
    }

    /*======================================================================+
         EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
         WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
     +======================================================================*/
    row_t * r_customer_local = get_row(r_customer, WR);
    if (r_customer_local == NULL) {
        return finish(Abort);
    }
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;

    r_customer_local->get_value(C_BALANCE, c_balance);
    r_customer_local->set_value(C_BALANCE, c_balance - params->h_amount);
    r_customer_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
    r_customer_local->set_value(C_YTD_PAYMENT, c_ytd_payment + params->h_amount);
    r_customer_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
    r_customer_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

    char * c_credit = r_customer_local->get_value(C_CREDIT);

    if ( strstr(c_credit, "BC") ) {
        //find out what to do here
    }

    //don't understand what exactly is going on here!
    char h_data[25];
    strncpy(h_data, w_name, 10);
    int length = static_cast<int>(strlen(h_data));
    if (length > 10) {
        length = 10;
    }
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length+14] = '\0';

    assert( rc == RCOK );
    return finish(rc);
}

RC TPCCTransactionManager::run_new_order(tpcc_new_order_params * query) {
    RC rc           = RCOK;
    uint64_t key    = 0;
    itemid_t * item = nullptr;

    bool        remote  = query->remote;
    uint64_t    w_id    = query->w_id;
    uint64_t    d_id    = query->d_id;
    uint64_t    c_id    = query->c_id;
    uint64_t    ol_cnt  = query->ol_cnt;

    /*=======================================================================+
        EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
        INTO :c_discount, :c_last, :c_credit, :w_tax
        FROM customer, warehouse
        WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
    +========================================================================*/
    item = index_read(db->i_warehouse, w_id, wh_to_part(w_id));
    assert(item != nullptr);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local = get_row(r_wh, RD);
    if (r_wh_local == nullptr) {
        return finish(Abort);
    }

    double w_tax;
    r_wh_local->get_value(W_TAX, w_tax);
    key = custKey(c_id, d_id, w_id);
    item = index_read(db->i_customer_id, key, wh_to_part(w_id));
    assert(item != nullptr);
    auto r_customer = (row_t *) item->location;
    auto r_customer_local = get_row(r_customer, RD);
    if (r_customer_local == NULL) {
        return finish(Abort);
    }

    uint64_t c_discount;
    r_customer_local->get_value(C_DISCOUNT, c_discount);

    /*==================================================+
        EXEC SQL SELECT d_next_o_id, d_tax
        INTO :d_next_o_id, :d_tax
        FROM district WHERE d_id = :d_id AND d_w_id = :w_id;

        EXEC SQL UPDATE district SET d _next_o_id = :d _next_o_id + 1
        WHERE d _id = :d_id AN D d _w _id = :w _id ;
    +===================================================*/

    key = distKey(d_id, w_id);
    item = index_read(db->i_district, key, wh_to_part(w_id));
    assert(item != nullptr);
    row_t * r_dist = ((row_t *)item->location);
    row_t * r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }

    double d_tax;
    r_dist_local->get_value(D_TAX, d_tax);

    int64_t o_id;
    o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
    o_id++;
    r_dist_local->set_value(D_NEXT_O_ID, o_id);

    /*========================================================================================+
        EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
        VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
    +========================================================================================*/

    row_t * r_order = nullptr;
    uint64_t row_id = 0;
    db->t_order->get_new_row(r_order, 0, row_id);
    r_order->set_value(O_ID, o_id);
    r_order->set_value(O_C_ID, c_id);
    r_order->set_value(O_D_ID, d_id);
    r_order->set_value(O_W_ID, w_id);
    r_order->set_value(O_ENTRY_D, 1234);
    r_order->set_value(O_OL_CNT, ol_cnt);
    int64_t all_local = (remote? 0 : 1);
    r_order->set_value(O_ALL_LOCAL, all_local);
    //insert_row(r_order, _wl->t_order);


    /*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
	row_t * r_new_order = nullptr;
    uint64_t r_new_order_id = 0;
	db->t_new_order->get_new_row(r_new_order, 0, r_new_order_id);
	r_new_order->set_value(NO_O_ID, o_id);
    r_new_order->set_value(NO_D_ID, d_id);
    r_new_order->set_value(NO_W_ID, w_id);
	//insert_row(r_new_order, db->t_new_order);

    for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {

        uint64_t ol_i_id        = query->items[ol_number].ol_i_id;
        uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        uint64_t ol_quantity    = query->items[ol_number].ol_quantity;
        /*===========================================+
            EXEC SQL SELECT i_price, i_name , i_data
            INTO :i_price, :i_name, :i_data
            FROM item
            WHERE i_id = :ol_i_id;
        +===========================================*/
        item = index_read(db->i_item, ol_i_id, 0);
        assert(item != nullptr);
        row_t * r_item = ((row_t *)item->location);
        row_t * r_item_local = get_row(r_item, RD);
        if (r_item_local == nullptr) {
            return finish(Abort);
        }

        int64_t i_price;
        r_item_local->get_value(I_PRICE, i_price);

        /*===================================================================*
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
        *=====================================================================*/

        uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
        itemid_t * stock_item;
        index_read(db->i_stock, stock_key, wh_to_part(ol_supply_w_id), stock_item);
        assert(item != nullptr);
        row_t * r_stock = ((row_t *)stock_item->location);
        row_t * r_stock_local = get_row(r_stock, WR);
        if (r_stock_local == nullptr) {
            return finish(Abort);
        }


        UInt64 s_quantity;
        int64_t s_remote_cnt;
        r_stock_local->get_value(S_QUANTITY, s_quantity);
#if !TPCC_SMALL
        int64_t s_ytd;
        int64_t s_order_cnt;
        r_stock_local->get_value(S_YTD, s_ytd);
        r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
        r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
        r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
#endif
        if (remote) {
            r_stock_local->get_value(S_REMOTE_CNT, s_remote_cnt);
            s_remote_cnt ++;
            r_stock_local->set_value(S_REMOTE_CNT, s_remote_cnt);
        }

        uint64_t quantity;
        if (s_quantity > ol_quantity + 10) {
            quantity = s_quantity - ol_quantity;
        } else {
            quantity = s_quantity - ol_quantity + 91;
        }
        r_stock_local->set_value(S_QUANTITY, quantity);

        /*======================================================================+
        EXEC SQL INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
            ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
        VALUES(:o_id, :d_id, :w_id, :ol_number,
            :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info);
        +========================================================================*/

        // XXX district info is not inserted.
//		row_t * r_ol;
//		uint64_t row_id;
//		_wl->t_order_line->get_new_row(r_ol, 0, row_id);
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
//		insert_row(r_ol, _wl->t_order_line);
    }
    assert( rc == RCOK );
    return finish(rc);
}



void TPCCExecutor::initialize(uint32_t num_threads, const char * path) {
    BenchmarkExecutor::initialize(num_threads, path);

    //Build database in parallel
    _db = new TPCCDatabase();
    _db->initialize(INIT_PARALLELISM);
    _db->load();

    //Load workload in parallel
    _loader = new TPCCWorkloadLoader();
    _loader->initialize(num_threads, _path);
    _loader->load();

    //Initialize each thread
    for(uint32_t i = 0; i < _num_threads; i++) {
        _threads[i].initialize(i, _db, _loader->get_queries_list(i), true);
    }
}


