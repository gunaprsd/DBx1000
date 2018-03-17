#include "row.h"
#include "catalog.h"
#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_hekaton.h"
#include "row_lock.h"
#include "row_mvcc.h"
#include "row_occ.h"
#include "row_silo.h"
#include "row_tictoc.h"
#include "row_ts.h"
#include "row_vll.h"
#include "table.h"
#include "txn.h"
#include <mm_malloc.h>

RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id) {
    _row_id = row_id;
    _part_id = part_id;
    this->table = host_table;
    Catalog *schema = host_table->get_schema();
    int tuple_size = schema->get_tuple_size();
    data = (char *)_mm_malloc(sizeof(char) * tuple_size, 64);
    return RCOK;
}
void row_t::init(int size) { data = (char *)_mm_malloc(size, 64); }

RC row_t::switch_schema(table_t *host_table) {
    this->table = host_table;
    return RCOK;
}

void row_t::init_manager(row_t *row) {
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    manager = (RowLock *)mem_allocator.alloc(sizeof(RowLock), _part_id);
#elif CC_ALG == TIMESTAMP
    manager = (Row_ts *)mem_allocator.alloc(sizeof(Row_ts), _part_id);
#elif CC_ALG == MVCC
    manager = (Row_mvcc *)_mm_malloc(sizeof(Row_mvcc), 64);
#elif CC_ALG == HEKATON
    manager = (Row_hekaton *)_mm_malloc(sizeof(Row_hekaton), 64);
#elif CC_ALG == OCC
    manager = (Row_occ *)mem_allocator.alloc(sizeof(Row_occ), _part_id);
#elif CC_ALG == TICTOC
    manager = (Row_tictoc *)_mm_malloc(sizeof(Row_tictoc), 64);
#elif CC_ALG == SILO
    manager = (Row_silo *)_mm_malloc(sizeof(Row_silo), 64);
#elif CC_ALG == VLL
    manager = (Row_vll *)mem_allocator.alloc(sizeof(Row_vll), _part_id);
#endif

#if CC_ALG != HSTORE
    manager->init(this);
#endif
}

table_t *row_t::get_table() { return table; }

Catalog *row_t::get_schema() { return get_table()->get_schema(); }

const char *row_t::get_table_name() { return get_table()->get_table_name(); };
uint64_t row_t::get_tuple_size() { return get_schema()->get_tuple_size(); }

uint64_t row_t::get_field_cnt() { return get_schema()->field_cnt; }

void row_t::set_value(int id, void *ptr) {
    int datasize = get_schema()->get_field_size(id);
    int pos = get_schema()->get_field_index(id);
    memcpy(&data[pos], ptr, datasize);
}

void row_t::set_value(int id, void *ptr, int size) {
    int pos = get_schema()->get_field_index(id);
    memcpy(&data[pos], ptr, size);
}

void row_t::set_value(const char *col_name, void *ptr) {
    uint64_t id = get_schema()->get_field_id(col_name);
    set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(uint32_t);
SET_VALUE(int32_t);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(uint32_t);
GET_VALUE(int32_t);

char *row_t::get_value(int id) {
    int pos = get_schema()->get_field_index(id);
    return &data[pos];
}

char *row_t::get_value(char *col_name) {
    uint64_t pos = get_schema()->get_field_index(col_name);
    return &data[pos];
}

char *row_t::get_data() { return data; }

void row_t::set_data(char *data, uint64_t size) { memcpy(this->data, data, size); }
// copy from the src to this
void row_t::copy(row_t *src) { set_data(src->get_data(), src->get_tuple_size()); }

void row_t::free_row() { free(data); }

// the "row" is the row read out in get_row().
// For locking based CC_ALG, the "row" is the same as "this".
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
void row_t::return_row(access_t type, txn_man *txn, row_t *row) {
#if CC_ALG == NONE
    return;
#elif CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
    assert(row == NULL || row == this || type == XP);
    if (ROLL_BACK && type == XP) { // recover from previous writes.
        this->copy(row);
    }
    this->manager->lock_release(txn);
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC
    // for RD or SCAN or XP, the row should be deleted.
    // because all WR should be companied by a RD
    // for MVCC RD, the row is not copied, so no need to free.
#if CC_ALG == TIMESTAMP
    if (type == RD || type == SCAN) {
        row->free_row();
        mem_allocator.free(row, sizeof(row_t));
    }
#endif
    if (type == XP) {
        this->manager->access(txn, XP_REQ, row);
    } else if (type == WR) {
        assert(type == WR && row != NULL);
        assert(row->get_schema() == this->get_schema());
        RC rc = this->manager->access(txn, W_REQ, row);
        assert(rc == RCOK);
    }
#elif CC_ALG == OCC
    assert(row != NULL);
    if (type == WR)
        manager->write(row, txn->end_ts);
    row->free_row();
    mem_allocator.free(row, sizeof(row_t));
    return;
#elif CC_ALG == TICTOC || CC_ALG == SILO
    assert(row != NULL);
    return;
#elif CC_ALG == HSTORE || CC_ALG == VLL
    return;
#else
    assert(false);
#endif
}

RC row_t::get_row(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == NONE
    return get_row_NONE(type, txn, row);
#elif CC_ALG == NO_WAIT
    return get_row_NO_WAIT(type, txn, row);
#elif CC_ALG == WAIT_DIE
    return get_row_WAIT_DIE(type, txn, row);
#elif CC_ALG == DL_DETECT
    return get_row_DL_DETECT(type, txn, row);
#elif CC_ALG == OCC
    return get_row_OCC(type, txn, row);
#elif CC_ALG == SILO
    return get_row_SILO(type, txn, row);
#elif CC_ALG == HEKATON
    return get_row_HEKATON(type, txn, row);
#elif CC_ALG == TIMESTAMP
    return get_row_TIMESTAMP(type, txn, row);
#elif CC_ALG == HSTORE
    return get_row_HSTORE(type, txn, row);
#elif CC_ALG == MVCC
    return get_row_MVCC(type, txn, row);
#elif CC_ALG == VLL
    return get_row_VLL(type, txn, row);
#endif
}

RC row_t::get_row_NONE(access_t type, txn_man *txn, row_t *&row) {
    row = this;
    return RCOK;
}
RC row_t::get_row_NO_WAIT(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == NO_WAIT
    lock_t lt = (type == RD || type == SCAN) ? LOCK_SH : LOCK_EX;
    RC rc = this->manager->lock_get(lt, txn);
    if (rc == RCOK) {
        row = this;
        return RCOK;
    } else {
        return Abort;
    }
#else
    return ERROR;
#endif
}

RC row_t::get_row_WAIT_DIE(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == WAIT_DIE
    uint64_t thread_id = txn->get_thd_id();
    lock_t lt = (type == RD || type == SCAN) ? LOCK_SH : LOCK_EX;
    RC rc = this->manager->lock_get(lt, txn);
    if (rc == RCOK) {
        row = this;
        return RCOK;
    } else if (rc == WAIT) {
        uint64_t start_time = get_sys_clock();
        while (!txn->lock_ready && !txn->lock_abort) {
            continue;
        }
        uint64_t end_time = get_sys_clock();
        INC_TMP_STATS(thread_id, time_wait, end_time - start_time);
        if (txn->lock_ready) {
            row = this;
            return RCOK;
        } else if (txn->lock_abort) {
            return_row(type, txn, nullptr);
            return Abort;
        } else {
            assert(false);
            return ERROR;
        }
    } else {
        // Abort
        return Abort;
    }
#else
    return ERROR;
#endif
}

RC row_t::get_row_DL_DETECT(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == DL_DETECT
    uint64_t thread_id = txn->get_thd_id();
    lock_t lt = (type == RD || type == SCAN) ? LOCK_SH : LOCK_EX;
    uint64_t *txnids;
    int txncnt;
    RC rc = manager->lock_get(lt, txn, txnids, txncnt);
    if (rc == RCOK) {
        row = this;
        return rc;
    } else if (rc == WAIT) {
        uint64_t start_time, last_detect, last_try, now;
        start_time = get_sys_clock();
        bool dependency_added = false;

        txn->lock_abort = false;
        INC_STATS(txn->get_thd_id(), wait_cnt, 1);
        last_detect = start_time;
        last_try = start_time;
        while (!txn->lock_ready && !txn->lock_abort) {
            now = get_sys_clock();

            /*
             * If have been waiting for a long time
             * (> timeout) abort gracefully.
             */
            if (now - start_time > g_timeout) {
                txn->lock_abort = true;
                break;
            }

            /*
             * (Periodically) try to add dependency and once added,
             * detect cycles until either ready or abort.
             */
            bool long_since_last_detect = (now - last_detect) > DL_LOOP_DETECT;
            bool long_since_last_try = (now - last_try) > DL_LOOP_TRIAL;
            if (long_since_last_try && long_since_last_detect) {
                if (!dependency_added) {
                    int okay = dl_detector.add_dep(txn->get_txn_id(), txnids, txncnt, txn->row_cnt);
                    if (okay == 0) {
                        dependency_added = true;
                    } else if (okay == 16) {
                        last_try = now;
                    }
                }

                if (dependency_added) {
                    int okay = dl_detector.detect_cycle(txn->get_txn_id());
                    if (okay == 16) {
                        last_try = now;
                    } else if (okay == 0) {
                        last_detect = now;
                    } else if (okay == 1) {
                        last_detect = now;
                    }
                }
            } else {
                PAUSE;
            }
        }

        if (txn->lock_ready) {
            row = this;
            return RCOK;
        } else if (txn->lock_abort) {
            return_row(type, txn, nullptr);
            return Abort;
        } else {
            return ERROR;
        }
        uint64_t end_time = get_sys_clock();
        INC_TMP_STATS(thread_id, time_wait, end_time - start_time);
    } else {
        return Abort;
    }
#else
    return ERROR;
#endif
}

RC row_t::get_row_OCC(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == OCC
    RC rc;
    txn->cur_row = (row_t *)mem_allocator.alloc(sizeof(row_t), get_part_id());
    txn->cur_row->init(get_table(), get_part_id());
    rc = this->manager->access(txn, R_REQ);
    row = txn->cur_row;
    return rc;
#else
    return ERROR;
#endif
}

RC row_t::get_row_TIMESTAMP(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == TIMESTAMP
    RC rc;
    uint64_t thd_id = txn->get_thd_id();
    txn->cur_row = (row_t *)mem_allocator.alloc(sizeof(row_t), this->get_part_id());
    txn->cur_row->init(get_table(), this->get_part_id());
    TsType ts_type = (type == RD) ? R_REQ : P_REQ;
    rc = this->manager->access(txn, ts_type, row);
    if (rc == RCOK) {
        row = txn->cur_row;
    } else if (rc == WAIT) {
        uint64_t start_time = get_sys_clock();
        while (!txn->ts_ready) {
            PAUSE
        }
        uint64_t end_time = get_sys_clock();
        INC_TMP_STATS(thd_id, time_wait, end_time - start_time);
        row = txn->cur_row;
    }

    if (rc != Abort) {
        row->table = get_table();
        assert(row->get_schema() == this->get_schema());
    }
    return rc;
#else
    return ERROR;
#endif
}

RC row_t::get_row_MVCC(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == MVCC
    RC rc;
    uint64_t thd_id = txn->get_thd_id();
    TsType ts_type = (type == RD) ? R_REQ : P_REQ;
    rc = this->manager->access(txn, ts_type, row);
    if (rc == RCOK) {
        row = txn->cur_row;
    } else if (rc == WAIT) {
        uint64_t t1 = get_sys_clock();
        while (!txn->ts_ready)
            PAUSE
        uint64_t t2 = get_sys_clock();
        INC_TMP_STATS(thd_id, time_wait, t2 - t1);
        row = txn->cur_row;
    }
    if (rc != Abort) {
        row->table = get_table();
        assert(row->get_schema() == this->get_schema());
    }
    return rc;
#else
    return ERROR;
#endif
}

RC row_t::get_row_TICTOC(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == TICTOC
    RC rc;
    row->table = get_table();
    TsType ts_type = (type == RD) ? R_REQ : P_REQ;
    rc = this->manager->access(txn, ts_type, row);
    return rc;
#else
    return ERROR;
#endif
}

RC row_t::get_row_SILO(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == SILO
    RC rc;
    row->table = get_table();
    TsType ts_type = (type == RD) ? R_REQ : P_REQ;
    rc = this->manager->access(txn, ts_type, row);
    return rc;
#else
    return ERROR;
#endif
}

RC row_t::get_row_HEKATON(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == HEKATON
    uint64_t thd_id = txn->get_thd_id();
    RC rc;
    TsType ts_type = (type == RD) ? R_REQ : P_REQ;
    rc = this->manager->access(txn, ts_type, row);
    if (rc == RCOK) {
        row = txn->cur_row;
    } else if (rc == WAIT) {
        uint64_t t1 = get_sys_clock();
        while (!txn->ts_ready)
            PAUSE
        uint64_t t2 = get_sys_clock();
        INC_TMP_STATS(thd_id, time_wait, t2 - t1);
        row = txn->cur_row;
    }
    if (rc != Abort) {
        row->table = get_table();
        assert(row->get_schema() == this->get_schema());
    }
    return rc;
#else
    return ERROR;
#endif
}

RC row_t::get_row_HSTORE(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == HSTORE
    row = this;
    return RCOK;
#else
    return ERROR;
#endif
}

RC row_t::get_row_VLL(access_t type, txn_man *txn, row_t *&row) {
#if CC_ALG == VLL
    row = this;
    return RCOK;
#else
    return ERROR;
#endif
}
