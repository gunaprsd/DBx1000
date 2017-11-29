#include "config.h"
#include "tpcc.h"
#include "row.h"

void TPCCDatabase::initialize(uint32_t num_threads) {
    Database::initialize(num_threads);

    //Initialize schema from the file
    string path;
    path = "./benchmarks/tpcc/schema.txt";
    Database::initialize_schema(path);

    //Allocate array to store rand buffer globally
    rand_buffer = new drand48_data * [_num_threads];

    //Obtain pointers for tables and indexes
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_new_order = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_order_line = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    i_item = indexes["ITEM_IDX"];
    i_warehouse = indexes["WAREHOUSE_IDX"];
    i_district = indexes["DISTRICT_IDX"];
    i_customer_id = indexes["CUSTOMER_ID_IDX"];
    i_customer_last = indexes["CUSTOMER_LAST_IDX"];
    i_stock = indexes["STOCK_IDX"];
}

txn_man *TPCCDatabase::get_txn_man(uint32_t thread_id) {
    auto manager = new TPCCTransactionManager();
    manager->initialize(this, thread_id);
    return manager;
}

void TPCCDatabase::load_items_table() {
    row_t * row = NULL;
    uint64_t row_id = 0;

    for (UInt32 i = 1; i <= g_max_items; i++) {
        //obtain a row from the table
        t_item->get_new_row(row, 0, row_id);

        //set primary key
        row->set_primary_key(i);

        //set other numeric fields
        row->set_value(I_ID, i);
        row->set_value(I_IM_ID, URand(1L,10000L, 0));
        row->set_value(I_PRICE, URand(1, 100, 0));

        //set name
        char name[24];
        MakeAlphaString(14, 24, name, 0);
        row->set_value(I_NAME, name);

        //set data: 10% has 'original', rest is random
        char data[50];
        MakeAlphaString(26, 50, data, 0);
        if (RAND(10, 0) == 0)
            strcpy(data, "original");
        row->set_value(I_DATA, data);

        //insert into index
        index_insert(i_item, i, row, 0);
    }
}

void TPCCDatabase::load_warehouse_table(uint32_t wid) {
    assert(wid >= 1 && wid <= g_num_wh);
    row_t * row = nullptr;
    uint64_t row_id = 0;

    //obtain a new row and row_id from table
    t_warehouse->get_new_row(row, 0, row_id);

    //set primary key
    row->set_primary_key(wid);
    row->set_value(W_ID, wid);

    //Generate other fields
    char name   [10];
    char street [20];
    char street2[20];
    char city   [20];
    char state  [2];
    char zip    [9];
    MakeAlphaString(6, 10, name, wid - 1);
    MakeAlphaString(10, 20, street, wid - 1);
    MakeAlphaString(10, 20, street2, wid - 1);
    MakeAlphaString(10, 20, city, wid - 1);
    MakeAlphaString(2, 2, state, wid - 1);
    MakeNumberString(9, 9, zip, wid - 1);
    double tax      = (double)URand(0L,200L,wid-1)/1000.0;
    double w_ytd    = 300000.00;

    //Set other fields
    row->set_value(W_NAME, name);
    row->set_value(W_STREET_1, street);
    row->set_value(W_STREET_2, street2);
    row->set_value(W_CITY, city);
    row->set_value(W_STATE, state);
    row->set_value(W_ZIP, zip);
    row->set_value(W_TAX, tax);
    row->set_value(W_YTD, w_ytd);

    //insert row into index
    index_insert(i_warehouse, wid, row, wh_to_part(wid));
}

void TPCCDatabase::load_districts_table(uint64_t wid) {
    row_t * row     = nullptr;
    uint64_t row_id = 0;
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        //Obtain new row from table
        t_district->get_new_row(row, 0, row_id);

        //Set primary key
        row->set_primary_key(did);
        row->set_value(D_ID, did);
        row->set_value(D_W_ID, wid);

        //Generate other fields
        char name   [10];
        char street [20];
        char street2[20];
        char city   [20];
        char state  [2];
        char zip    [9];
        MakeAlphaString(6, 10, name, wid - 1);
        MakeAlphaString(10, 20, street, wid - 1);
        MakeAlphaString(10, 20, street2, wid - 1);
        MakeAlphaString(10, 20, city, wid - 1);
        MakeAlphaString(2, 2, state, wid - 1);
        MakeNumberString(9, 9, zip, wid - 1);
        double tax      = (double)URand(0L,200L,wid-1)/1000.0;
        double w_ytd    = 30000.00;

        //Set other fields
        row->set_value(D_NAME, name);
        row->set_value(D_STREET_1, street);
        row->set_value(D_STREET_2, street2);
        row->set_value(D_CITY, city);
        row->set_value(D_STATE, state);
        row->set_value(D_ZIP, zip);
        row->set_value(D_TAX, tax);
        row->set_value(D_YTD, w_ytd);
        row->set_value(D_NEXT_O_ID, 3001);

        //Insert into index with a combined key (wid, did) and with part id = wid
        index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
    }
}

void TPCCDatabase::load_stocks_table(uint64_t wid) {
    row_t * row     = nullptr;
    uint64_t row_id = 0;
    for (UInt32 sid = 1; sid <= g_max_items; sid++) {
        //Obtain a new row from table
        t_stock->get_new_row(row, 0, row_id);

        //Set primary key and ids
        row->set_primary_key(sid);
        row->set_value(S_I_ID, sid);
        row->set_value(S_W_ID, wid);

        row->set_value(S_QUANTITY, URand(10, 100, wid-1));
        row->set_value(S_REMOTE_CNT, 0);

#if !TPCC_SMALL
        //Setting s_dist for all 10
        char s_dist[25];
        int row_names [10] = {
                S_DIST_01,
                S_DIST_02,
                S_DIST_03,
                S_DIST_04,
                S_DIST_05,
                S_DIST_06,
                S_DIST_07,
                S_DIST_08,
                S_DIST_09,
                S_DIST_10
        };
        for (int row_name : row_names) {
            MakeAlphaString(24, 24, s_dist, wid - 1);
            row->set_value(row_name, s_dist);
        }
        row->set_value(S_YTD, 0);
        row->set_value(S_ORDER_CNT, 0);

        //Setting s_data to a random string in which 10% contains 'original' as last 8 letters
        char s_data[50];
        auto len = (int)MakeAlphaString(26, 50, s_data, wid-1);
        if (rand() % 100 < 10) {
            auto idx = (int)URand(0, (uint64_t)(len - 8), wid-1);
            strcpy(& s_data[idx], "original");
        }
        row->set_value(S_DATA, s_data);
#endif
        //Inserting with a complex key and into wid partition
        index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
    }
}

void TPCCDatabase::load_customer_table(uint64_t did, uint64_t wid) {
    assert(g_cust_per_dist >= 1000);
    row_t * row     = nullptr;
    uint64_t row_id = 0;
    for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
        //Obtain new row
        t_customer->get_new_row(row, 0, row_id);

        //set primary key and other ids
        row->set_primary_key(cid);
        row->set_value(C_ID, cid);
        row->set_value(C_D_ID, did);
        row->set_value(C_W_ID, wid);

        //set last name
        char c_last[LASTNAME_LEN];
        if (cid <= 1000) {
            Lastname(cid - 1, c_last);
        } else {
            Lastname(NURand(255, 0, 999, wid - 1), c_last);
        }
        row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
        char tmp[3] = "OE";
        char c_first[FIRSTNAME_LEN];
        char street[20];
        char street2[20];
        char city[20];
        char state[2];
        char zip[9];
        char phone[16];
        char c_data[500];

        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
        MakeAlphaString(10, 20, street, wid - 1);
        MakeAlphaString(10, 20, street2, wid - 1);
        MakeAlphaString(10, 20, city, wid - 1);
        MakeAlphaString(2, 2, state, wid - 1);
        MakeNumberString(9, 9, zip, wid - 1);
        MakeNumberString(16, 16, phone, wid - 1);
        MakeAlphaString(300, 500, c_data, wid - 1);

        row->set_value(C_MIDDLE, tmp);
        row->set_value(C_FIRST, c_first);
        row->set_value(C_STREET_1, street);
        row->set_value(C_STREET_2, street2);
        row->set_value(C_CITY, city);
        row->set_value(C_STATE, state);
        row->set_value(C_ZIP, zip);
        row->set_value(C_PHONE, phone);
        row->set_value(C_SINCE, 0);
        row->set_value(C_CREDIT_LIM, 50000);
        row->set_value(C_DELIVERY_CNT, 0);
        row->set_value(C_DATA, c_data);
#endif

        if (RAND(10, wid-1) == 0) {
            strcpy(tmp, "GC");
            row->set_value(C_CREDIT, tmp);
        } else {
            strcpy(tmp, "BC");
            row->set_value(C_CREDIT, tmp);
        }
        row->set_value(C_DISCOUNT, (double)RAND(5000, wid - 1) / 10000);
        row->set_value(C_BALANCE, -10.0);
        row->set_value(C_YTD_PAYMENT, 10.0);
        row->set_value(C_PAYMENT_CNT, 1);

        //Insert into primary index - cid
        index_insert(i_customer_id, custKey(cid, did, wid), row, wh_to_part(wid));
        //Insert into seconday index - last_name
        index_insert(i_customer_last, custNPKey(c_last, did, wid), row, wh_to_part(wid));
    }
}

void TPCCDatabase::load_history_table(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
    row_t * row     = nullptr;
    uint64_t row_id = 0;

    //Obtain row from table
    t_history->get_new_row(row, 0, row_id);

    //Set primary key and other ids
    row->set_primary_key(0);
    row->set_value(H_C_ID, c_id);
    row->set_value(H_C_D_ID, d_id);
    row->set_value(H_D_ID, d_id);
    row->set_value(H_C_W_ID, w_id);
    row->set_value(H_W_ID, w_id);

    //Set other fields
    row->set_value(H_DATE, 0);
    row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
    char h_data[24];
    MakeAlphaString(12, 24, h_data, w_id-1);
    row->set_value(H_DATA, h_data);
#endif

    //No index on history table
}

void TPCCDatabase::load_order_table(uint64_t did, uint64_t wid) {
    /* initialize permutation of customer numbers */
    uint64_t perm[g_cust_per_dist];
    initialize_permutation(perm, wid);

    row_t *     row     = nullptr;
    uint64_t    row_id  = 0;

    for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
        //Obtain an order row from table
        t_order->get_new_row(row, 0, row_id);

        //set primary key
        row->set_primary_key(oid);
        row->set_value(O_ID, oid);
        row->set_value(O_D_ID, did);
        row->set_value(O_W_ID, wid);

        //obtain a customer id from permutation
        uint64_t cid = perm[oid - 1];
        row->set_value(O_C_ID, cid);

        //set other minor fields
        uint64_t o_entry = 2013;
        row->set_value(O_ENTRY_D, o_entry);
        if (oid < 2101) {
            row->set_value(O_CARRIER_ID, URand(1, 10, wid - 1));
        } else {
            row->set_value(O_CARRIER_ID, 0);
        }

        //Obtain random number of order_line and set all local
        uint64_t o_ol_cnt = URand(5, 15, wid-1);
        row->set_value(O_OL_CNT, o_ol_cnt);
        row->set_value(O_ALL_LOCAL, 1);


#if !TPCC_SMALL
        //Insert as many order_lines into the table
        row_t *     ol_row      = nullptr;
        uint64_t    ol_row_id   = 0;

        for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
            //obtain order line row
            t_order_line->get_new_row(ol_row, 0, ol_row_id);
            //set ids and other values
            ol_row->set_value(OL_O_ID, oid);
            ol_row->set_value(OL_D_ID, did);
            ol_row->set_value(OL_W_ID, wid);
            ol_row->set_value(OL_NUMBER, ol);
            ol_row->set_value(OL_SUPPLY_W_ID, wid);

            //set item id and quantity
            ol_row->set_value(OL_I_ID, URand(1, 100000, wid - 1));
            ol_row->set_value(OL_QUANTITY, 5);

            //set delivery and amount information
            if (oid < 2101) {
                ol_row->set_value(OL_DELIVERY_D, o_entry);
                ol_row->set_value(OL_AMOUNT, 0);
            } else {
                ol_row->set_value(OL_DELIVERY_D, 0);
                ol_row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid - 1)/100);
            }
            //set district information
            char ol_dist_info[24];
            MakeAlphaString(24, 24, ol_dist_info, wid-1);
            ol_row->set_value(OL_DIST_INFO, ol_dist_info);
        }
#endif
        //Insert into new order table
        row_t *     new_order_row       = nullptr;
        uint64_t    new_order_row_id    = 0;
        if (oid > 2100) {
            t_new_order->get_new_row(new_order_row, 0, new_order_row_id);
            new_order_row->set_value(NO_O_ID, oid);
            new_order_row->set_value(NO_D_ID, did);
            new_order_row->set_value(NO_W_ID, wid);
        }
    }
}

void TPCCDatabase::initialize_permutation(uint64_t * perm_c_id, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for(i = 0; i < g_cust_per_dist; i++)
        perm_c_id[i] = i+1;

    // shuffle
    for(i=0; i < g_cust_per_dist-1; i++) {
        uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}

void TPCCDatabase::load_tables(uint32_t thread_id) {
    //thread i loads warehouse information for wid = (i+1)
    uint32_t wid = thread_id + 1;
    //initialize rand_buffer
    rand_buffer[thread_id] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
    assert((uint64_t)thread_id < g_num_wh);
    srand48_r(wid, rand_buffer[thread_id]);

    //only one thread generates items table
    if (thread_id == 0) {
        load_items_table();
    }


    load_warehouse_table( wid );
    load_districts_table( wid );
    load_stocks_table( wid );
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        load_customer_table(did, wid);
        load_order_table(did, wid);
        for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) {
            load_history_table(cid, did, wid);
        }
    }
}
