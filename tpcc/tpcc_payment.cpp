#include "tpcc_database.h"
#include "config.h"
#include "row.h"
#include "table.h"

void TPCCWorkloadGenerator::gen_payment_request(uint64_t thread_id, tpcc_query *query) {
    auto params = reinterpret_cast<tpcc_payment_params *>(&query->params);

    // Choose warehouse
    if (FIRST_PART_LOCAL) {
        params->w_id = (thread_id % config.num_warehouses) + 1;
    } else {
        params->w_id = (_random.nextInt64(thread_id) % config.num_warehouses) + 1;
    }

    // Choose district
    params->d_w_id = params->w_id;
    params->d_id = helper.generateRandom(1, config.districts_per_warehouse, params->w_id - 1);

    // Choose customer
    params->c_w_id = params->w_id;
    params->c_d_id = params->d_id;
    params->c_id = helper.generateNonUniformRandom(1023, 1, config.customers_per_district, params->w_id - 1);
    params->by_last_name = false;

    // Choose amount
    params->h_amount = helper.generateRandom(1, 5000, params->w_id - 1);

/*
    auto x = (int)helper.generateRandom(1, 100, params->w_id - 1);
    if (x > FLAGS_tpcc_remote_payment_percent) {
        // home warehouse
        params->c_d_id = params->d_id;
        params->c_w_id = params->w_id;
    } else {
        // remote warehouse
        params->c_d_id = helper.generateRandom(1, config.districts_per_warehouse, params->w_id - 1);
        if (config.num_warehouses > 1) {
            // generate something other than params->w_id
            while ((params->c_w_id = helper.generateRandom(1, config.num_warehouses,
                                                           params->w_id - 1)) == params->w_id) {
            }
        } else {
            params->c_w_id = params->w_id;
        }
    }

    auto y = (int)helper.generateRandom(1, 100, params->w_id - 1);
    if (y <= 100 * FLAGS_tpcc_by_last_name_percent) {
        // by last name
        params->by_last_name = true;
        TPCCUtility::findLastNameForNum(
            helper.generateNonUniformRandom(255, 0, 999, params->w_id - 1), params->c_last);
    } else {
        // by customer id
        params->by_last_name = false;
        params->c_id = helper.generateNonUniformRandom(1023, 1, config.customers_per_district,
                                                       params->w_id - 1);
    }
*/

}

/*
 * - Reading warehouse tuple from warehouse table (for optional update)
 * - Reading district tuple for update
 * - Reading customer tuple (either through last name or customer id) for update
 */
RC TPCCTransactionManager::run_payment(tpcc_payment_params *params) {
    uint32_t access_id = 0;
    RC rc = RCOK;
    itemid_t *item = nullptr;
    uint64_t key = 0;

    uint64_t w_id = params->w_id;
    uint64_t c_w_id = params->c_w_id;

    /*====================================================+
      EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
      WHERE w_id=:w_id;
    +====================================================*/

    item = index_read(db->i_warehouse, params->w_id, TPCCUtility::getPartition(params->w_id));
    assert(item != nullptr);
    row_t *r_wh = ((row_t *)item->location);
    row_t *r_wh_local = nullptr;
    r_wh_local = get_row(r_wh, db->config.warehouse_update ? WR : RD);
    if (r_wh_local == nullptr) {
        return finish(Abort);
    }
    access_id++;
    assert(r_wh_local != nullptr);

    if (db->config.warehouse_update) {
        double w_ytd;
        r_wh_local->get_value(W_YTD, w_ytd);
        r_wh_local->set_value(W_YTD, w_ytd + params->h_amount);
    }

    /*===================================================================+
      EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state,
      w_zip, w_name INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip,
      :w_name FROM warehouse WHERE w_id=:w_id;
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

    /*=====================================================+
      EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
      WHERE d_w_id=:w_id AND d_id=:d_id;
    +=====================================================*/

    key = TPCCUtility::getDistrictKey(params->d_id, params->d_w_id);
    item = index_read(db->i_district, key, TPCCUtility::getPartition(w_id));
    assert(item != nullptr);
    row_t *r_dist = ((row_t *)item->location);
    row_t *r_dist_local = nullptr;
    r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == nullptr) {
        return finish(Abort);
    }
    access_id++;

    double d_ytd;
    r_dist_local->get_value(D_YTD, d_ytd);
    r_dist_local->set_value(D_YTD, d_ytd + params->h_amount);

    /*====================================================================+
      EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state,
      d_zip, d_name INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip,
      :d_name FROM district WHERE d_w_id=:w_id AND d_id=:d_id;
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

    row_t *r_customer;
    if (params->by_last_name) {
        /*=======================================================================*
          EXEC SQL SELECT count(c_id) INTO :name_cnt
          FROM customer
          WHERE c_last=:c_last AND c_d_id=:c_d_id AND
          c_w_id=:c_w_id;
        +========================================================================*/

        /*==========================================================================+
          EXEC SQL DECLARE c_by_name CURSOR FOR
          SELECT c_first, c_middle, c_id, c_street_1,
          c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
          c_discount, c_balance, c_since FROM customer WHERE c_w_id=:c_w_id AND
          c_d_id=:c_d_id AND c_last=:c_last ORDER BY c_first; EXEC SQL OPEN
          c_by_name;
        +===========================================================================*/

        key = TPCCUtility::getCustomerLastNameKey(params->c_last, params->c_d_id, params->c_w_id);
        item = index_read(db->i_customer_last, key, TPCCUtility::getPartition(c_w_id));
        assert(item != nullptr);
        int cnt = 0;
        itemid_t *it = item;
        itemid_t *mid = item;
        while (it != nullptr) {
            cnt++;
            it = it->next;
            if (cnt % 2 == 0) {
                mid = mid->next;
            }
        }
        r_customer = ((row_t *)mid->location);
    } else {
        /*======================================================================+
          EXEC SQL UPDATE customer SET c_balance = :c_balance,
          c_data = :c_new_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
        +======================================================================*/

        key = TPCCUtility::getCustomerPrimaryKey(params->c_id, params->c_d_id, params->c_w_id);
        item = index_read(db->i_customer_id, key, TPCCUtility::getPartition(c_w_id));
        assert(item != NULL);
        r_customer = (row_t *)item->location;
    }

    row_t *r_customer_local = nullptr;
    r_customer_local = get_row(r_customer, WR);
    if (r_customer_local == NULL) {
        return finish(Abort);
    }
    access_id++;

    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;

    r_customer_local->get_value(C_BALANCE, c_balance);
    r_customer_local->set_value(C_BALANCE, c_balance - params->h_amount);
    r_customer_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
    r_customer_local->set_value(C_YTD_PAYMENT, c_ytd_payment + params->h_amount);
    r_customer_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
    r_customer_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

    char *c_credit = r_customer_local->get_value(C_CREDIT);

    if (strstr(c_credit, "BC")) {
        // find out what to do here
    }

    // don't understand what exactly is going on here!
    char h_data[25];
    strncpy(h_data, w_name, 10);
    int length = static_cast<int>(strlen(h_data));
    if (length > 10) {
        length = 10;
    }
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length + 14] = '\0';

    assert(rc == RCOK);
    return finish(rc);
}
