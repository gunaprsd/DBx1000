#include "vll.h"
#include "catalog.h"
#include "mem_alloc.h"
#include "row.h"
#include "row_vll.h"
#include "table.h"
#include "txn.h"
#include "ycsb_database.h"
#if CC_ALG == VLL

void VLLMan::init() {
  _txn_queue_size = 0;
  _txn_queue = NULL;
  _txn_queue_tail = NULL;
}

void VLLMan::vllMainLoop(txn_man *txn, BaseQuery *query) {

  ycsb_params *m_query = &((ycsb_query *)query)->params;
  // access the indexes. This is not in the critical section
  for (uint32_t rid = 0; rid < m_query->request_cnt; rid++) {
    ycsb_request *req = &m_query->requests[rid];
    YCSBDatabase *wl = (YCSBDatabase *)txn->get_db();
    int part_id = wl->key_to_part(req->key);
    INDEX *index = wl->the_index;
    itemid_t *item;
    item = txn->index_read(index, req->key, part_id);
    row_t *row = ((row_t *)item->location);
    // the following line adds the read/write sets to txn->accesses
    txn->get_row(row, req->rtype);
    // int cs = row->manager->get_cs();
  }

  bool done = false;
  while (!done) {
    txn_man *front_txn = NULL;
    pthread_mutex_lock(&_mutex);

    TxnQEntry *front = _txn_queue;
    if (front)
      front_txn = front->txn;
    // only one worker thread can execute the txn.
    if (front_txn && front_txn->vll_txn_type == VLL_Blocked) {
      front_txn->vll_txn_type = VLL_Free;
      pthread_mutex_unlock(&_mutex);
      execute(front_txn, query);
      finishTxn(front_txn, front);
    } else {
      // _mutex will be unlocked in beginTxn()
      TxnQEntry *entry = NULL;
      int ok = beginTxn(txn, query, entry);
      if (ok == 2) {
        execute(txn, query);
        finishTxn(txn, entry);
      }
      assert(ok == 1 || ok == 2);
      done = true;
    }
  }
  return;
}

int VLLMan::beginTxn(txn_man *txn, BaseQuery *query, TxnQEntry *&entry) {

  int ret = -1;
  if (_txn_queue_size >= TXN_QUEUE_SIZE_LIMIT)
    ret = 3;

  txn->vll_txn_type = VLL_Free;

  for (int rid = 0; rid < txn->row_cnt; rid++) {
    access_t type = txn->accesses[rid]->type;
    if (txn->accesses[rid]->orig_row->manager->insert_access(type))
      txn->vll_txn_type = VLL_Blocked;
  }

  entry = getQEntry();
  LIST_PUT_TAIL(_txn_queue, _txn_queue_tail, entry);
  if (txn->vll_txn_type == VLL_Blocked)
    ret = 1;
  else
    ret = 2;
  pthread_mutex_unlock(&_mutex);
  return ret;
}

void VLLMan::execute(txn_man *txn, BaseQuery *query) {
  // RC rc;
  // ycsb_params * m_query = & ((ycsb_query *) query)->params;
  YCSBDatabase *wl = (YCSBDatabase *)txn->get_db();
  Catalog *schema = wl->the_table->get_schema();
  // uint64_t average;
  for (int rid = 0; rid < txn->row_cnt; rid++) {
    row_t *row = txn->accesses[rid]->orig_row;
    access_t type = txn->accesses[rid]->type;
    if (type == RD) {
      for (uint32_t fid = 0; fid < schema->get_field_cnt(); fid++) {
        // char * data = row->get_data();
        // uint64_t fval = *(uint64_t *)(&data[fid * 100]);
      }
    } else {
      assert(type == WR);
      for (uint32_t fid = 0; fid < schema->get_field_cnt(); fid++) {
        char *data = row->get_data();
        *(uint64_t *)(&data[fid * 100]) = 0;
      }
    }
  }
}

void VLLMan::finishTxn(txn_man *txn, TxnQEntry *entry) {
  pthread_mutex_lock(&_mutex);

  for (int rid = 0; rid < txn->row_cnt; rid++) {
    access_t type = txn->accesses[rid]->type;
    txn->accesses[rid]->orig_row->manager->remove_access(type);
  }
  LIST_REMOVE_HT(entry, _txn_queue, _txn_queue_tail);
  pthread_mutex_unlock(&_mutex);
  txn->release();
  mem_allocator.free(txn, 0);
}

TxnQEntry *VLLMan::getQEntry() {
  TxnQEntry *entry = (TxnQEntry *)mem_allocator.alloc(sizeof(TxnQEntry), 0);
  entry->prev = NULL;
  entry->next = NULL;
  entry->txn = NULL;
  return entry;
}

void VLLMan::returnQEntry(TxnQEntry *entry) {
  mem_allocator.free(entry, sizeof(TxnQEntry));
}

#endif
