#include "config.h"
#include "row.h"
#include "table.h"
#include "tpcc_database.h"
#include "tpcc_workload.h"

void TPCCWorkloadGenerator::gen_new_order_request(uint64_t thread_id, tpcc_query *query) {
    auto params = reinterpret_cast<tpcc_new_order_params *>(&query->params);
    // choose a home warehouse
    if (FIRST_PART_LOCAL) {
        params->w_id = (thread_id % config.num_warehouses) + 1;
    } else {
        params->w_id = (_random.nextInt64(thread_id) % config.num_warehouses) + 1;
    }

    // choose a district
    params->d_id = helper.generateRandom(1, config.districts_per_warehouse, params->w_id - 1);

    // choose a customer
    params->c_id =
        helper.generateNonUniformRandom(1023, 1, config.customers_per_district, params->w_id - 1);

    params->rbk = (bool)helper.generateRandom(1, 100, params->w_id - 1);

    params->o_entry_d = 2013;
    if (TPCC_NUM_ORDERS_RANDOM) {
        params->ol_cnt =
            helper.generateRandom(TPCC_MIN_NUM_ORDERS, TPCC_MAX_NUM_ORDERS, params->w_id - 1);
    } else {
        params->ol_cnt = TPCC_MAX_NUM_ORDERS;
    }

    params->remote = false;
    // Generate
    for (uint32_t oid = 0; oid < params->ol_cnt; oid++) {
        // choose a unique random item
        bool unique;
        do {
            params->items[oid].ol_i_id =
                helper.generateRandom(1, config.items_count, params->w_id - 1);
            unique = true;
            for (uint32_t i = 0; i < oid; i++) {
                if (params->items[i].ol_i_id == params->items[oid].ol_i_id) {
                    unique = false;
                    break;
                }
            }
        } while (!unique);

        // 1% of ol items go remote
        // params->items[oid].ol_supply_w_id = params->w_id;
        // params->items[oid].ol_quantity = helper.generateRandom(1, 10, params->w_id - 1);

        auto x = (uint32_t)helper.generateRandom(1, 100, params->w_id - 1);
        if (x > 1 || config.num_warehouses == 1) {
            params->items[oid].ol_supply_w_id = params->w_id;
        } else {
            while ((params->items[oid].ol_supply_w_id = helper.generateRandom(
                        1, config.num_warehouses, params->w_id - 1)) == params->w_id) {
            }
            params->remote = true;
        }
        params->items[oid].ol_quantity = helper.generateRandom(1, 10, params->w_id - 1);
    }

    for (uint32_t i = 0; i < params->ol_cnt; i++) {
        for (uint32_t j = 0; j < i; j++) {
            assert(params->items[(int)i].ol_i_id != params->items[(int)j].ol_i_id);
        }
    }

    num_orders[thread_id] += params->ol_cnt;
    num_order_txns[thread_id] += 1;
}

/*
 * - Read warehouse tuple
 * - Read district tuple for update
 * - Read customer tuple using cid
 * - Insert order tuple
 * for each order line tuples:
 *    - read item tuple
 *    - read stocks tuple for update
 *    - insert new order line tuple
 */
RC TPCCTransactionManager::run_new_order(tpcc_new_order_params *query) {
    uint32_t access_id = 0;
    RC rc = RCOK;
    uint64_t key = 0;
    itemid_t *item = nullptr;

    bool remote = query->remote;
    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;

    /*=======================================================================+
      EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
      INTO :c_discount, :c_last, :c_credit, :w_tax
      FROM customer, warehouse
      WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id
      AND c_id = :c_id;
    +========================================================================*/
    item = index_read(db->i_warehouse, w_id, TPCCUtility::getPartition(w_id));
    assert(item != nullptr);
    row_t *r_wh = ((row_t *)item->location);
    row_t *r_wh_local = nullptr;
    r_wh_local = get_row(r_wh, RD);
    if (r_wh_local == nullptr) {
        return finish(Abort);
    }
    assert(r_wh_local != nullptr);
    access_id++;

    /*========================================================================+
      EXEC SQL SELECT d_next_o_id, d_tax
      INTO :d_next_o_id, :d_tax
      FROM district WHERE d_id = :d_id AND d_w_id = :w_id;

      EXEC SQL UPDATE district SET d _next_o_id = :d
      _next_o_id + 1 WHERE d _id = :d_id AN D d _w _id = :w _id ;
    +=========================================================================*/

    key = TPCCUtility::getDistrictKey(d_id, w_id);
    item = index_read(db->i_district, key, TPCCUtility::getPartition(w_id));
    assert(item != nullptr);
    row_t *r_dist = ((row_t *)item->location);
    row_t *r_dist_local = nullptr;
    r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == nullptr) {
        return finish(Abort);
    }
    assert(r_dist_local != nullptr);
    access_id++;

    double w_tax;
    r_wh_local->get_value(W_TAX, w_tax);
    key = TPCCUtility::getCustomerPrimaryKey(c_id, d_id, w_id);
    item = index_read(db->i_customer_id, key, TPCCUtility::getPartition(w_id));
    assert(item != nullptr);
    auto r_customer = (row_t *)item->location;
    row_t *r_customer_local = nullptr;
    if (SELECTIVE_CC && query->cc_info[access_id] == 0) {
        r_customer_local = r_customer;
    } else {
        r_customer_local = get_row(r_customer, RD);
        if (r_customer_local == NULL) {
            return finish(Abort);
        }
    }
    assert(r_customer_local != nullptr);
    access_id++;

    uint64_t c_discount;
    r_customer_local->get_value(C_DISCOUNT, c_discount);

    double d_tax;
    r_dist_local->get_value(D_TAX, d_tax);

    int64_t o_id;
    o_id = *(int64_t *)r_dist_local->get_value(D_NEXT_O_ID);
    o_id++;
    r_dist_local->set_value(D_NEXT_O_ID, o_id);

    /*================================================================================+
      EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id,
      o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (:o_id, :d_id, :w_id,
      :c_id, :datetime, :o_ol_cnt, :o_all_local);
    +================================================================================*/

    row_t *r_order = nullptr;
    uint64_t row_id = 0;
    db->t_order->get_new_row(r_order, 0, row_id);
    r_order->set_value(O_ID, o_id);
    r_order->set_value(O_C_ID, c_id);
    r_order->set_value(O_D_ID, d_id);
    r_order->set_value(O_W_ID, w_id);
    r_order->set_value(O_ENTRY_D, 1234);
    r_order->set_value(O_OL_CNT, ol_cnt);
    int64_t all_local = (remote ? 0 : 1);
    r_order->set_value(O_ALL_LOCAL, all_local);
    // insert_row(r_order, _wl->t_order);

    /*=======================================================+
      EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
      VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
    row_t *r_new_order = nullptr;
    uint64_t r_new_order_id = 0;
    db->t_new_order->get_new_row(r_new_order, 0, r_new_order_id);
    r_new_order->set_value(NO_O_ID, o_id);
    r_new_order->set_value(NO_D_ID, d_id);
    r_new_order->set_value(NO_W_ID, w_id);
    // insert_row(r_new_order, db->t_new_order);

    for (uint32_t ol_number = 0; ol_number < ol_cnt; ol_number++) {

        uint64_t ol_i_id = query->items[ol_number].ol_i_id;
        uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        uint64_t ol_quantity = query->items[ol_number].ol_quantity;
        /*===========================================+
         EXEC SQL SELECT i_price, i_name , i_data
         INTO :i_price, :i_name, :i_data
         FROM item
         WHERE i_id = :ol_i_id;
        +===========================================*/
        item = index_read(db->i_item, ol_i_id, 0);
        assert(item != nullptr);
        row_t *r_item = ((row_t *)item->location);
        row_t *r_item_local = nullptr;
        r_item_local = get_row(r_item, RD);
        if (r_item_local == nullptr) {
            return finish(Abort);
        }
        assert(r_item_local != nullptr);
        access_id++;

        int64_t i_price;
        r_item_local->get_value(I_PRICE, i_price);

        /*===================================================================*
         EXEC SQL SELECT s_quantity, s_data,
         s_dist_01, s_dist_02, s_dist_03,
         s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09,
         s_dist_10 INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02, :s_dist_03,
         :s_dist_04, :s_dist_05, :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09,
         :s_dist_10 FROM stock WHERE s_i_id = :ol_i_id AND s_w_id =
         :ol_supply_w_id;

         EXEC SQL UPDATE stock SET s_quantity =
         :s_quantity WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
        *=====================================================================*/

        uint64_t stock_key = TPCCUtility::getStockKey(ol_i_id, ol_supply_w_id);
        itemid_t *stock_item;
        index_read(db->i_stock, stock_key, TPCCUtility::getPartition(ol_supply_w_id), stock_item);
        assert(item != nullptr);
        row_t *r_stock = ((row_t *)stock_item->location);
        row_t *r_stock_local = nullptr;
        r_stock_local = get_row(r_stock, WR);
        if (r_stock_local == nullptr) {
            return finish(Abort);
        }
        assert(r_stock_local != nullptr);
        access_id++;

        uint64_t s_quantity;
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
            s_remote_cnt++;
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
          EXEC SQL INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id,
          ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
          VALUES(:o_id, :d_id, :w_id, :ol_number, :ol_i_id, :ol_supply_w_id, :ol_quantity,
          :ol_amount, :ol_dist_info);
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
//		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax)
//* (1 - c_discount); 		r_ol->set_value(OL_SUPPLY_W_ID,
//&ol_supply_w_id); 		r_ol->set_value(OL_QUANTITY, &ol_quantity);
//		r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif
        //		insert_row(r_ol, _wl->t_order_line);
    }
    assert(rc == RCOK);
    return finish(rc);
}
