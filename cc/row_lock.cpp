#include "row_lock.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"

void RowLock::init(row_t *row) {
  _row = row;
  owners.init();
  waiters.init();
  latch = new pthread_mutex_t;
  pthread_mutex_init(latch, NULL);
  lock_type = LOCK_NONE;
}

RC RowLock::lock_get(lock_t type, txn_man *txn) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt);
}

RC RowLock::lock_get(lock_t type, txn_man *txn, uint64_t *&txnids,
                     int &txncnt) {
  assert(CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE ||
         CC_ALG == NONE);

  RC rc = RCOK;

#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT
  get_latch();

  bool conflict = LockEntry::conflict_lock(lock_type, type);
#if CC_ALG == NO_WAIT

  if (conflict) {
    rc = Abort;
  }

#elif CC_ALG == WAIT_DIE

  if (conflict) {
    bool can_wait = owners.can_wait(txn->get_ts());
    if (can_wait) {
      LockEntry *entry = get_entry();
      entry->txn = txn;
      entry->type = type;
      waiters.insert(entry);
      txn->lock_ready = false;
      rc = WAIT;
    } else {
      rc = Abort;
    }
  }

#elif CC_ALG == DL_DETECT

  if (conflict) {
    // create a lock entry
    LockEntry *entry = get_entry();
    entry->txn = txn;
    entry->type = type;
    waiters.push(entry);

    // update the waits-for graph!
    auto part_id = _row->get_part_id();
    txnids = reinterpret_cast<uint64_t *>(mem_allocator.alloc(
        sizeof(uint64_t) * (owners.size() + waiters.size()), part_id));
    waiters.add_dependencies(entry, txnids, txncnt);
    if (LockEntry::conflict_lock(type, lock_type)) {
      owners.add_dependencies(entry, txnids, txncnt);
    }
    assert(txncnt > 0);

    txn->lock_ready = false;
    rc = WAIT;
  }

#endif
  else {
    // No conflict!
    LockEntry *entry = get_entry();
    entry->type = type;
    entry->txn = txn;
    owners.push(entry);
    lock_type = type;
    rc = RCOK;
  }

  // Unlock and return rc
  release_latch();
#endif

  return rc;
}

RC RowLock::lock_release(txn_man *txn) {
  RC rc = RCOK;
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT
  get_latch();
  // try to find the entry in owners list
  LockEntry *entry = nullptr;
  if (owners.remove(txn, entry)) {
    return_entry(entry);
    if (owners.size() == 0) {
      lock_type = LOCK_NONE;
    }
  }
#if CC_ALG == NO_WAIT
  else {
    assert(false);
  }
#elif CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT
  else {
    waiters.remove(txn, entry);
    return_entry(entry);
  }
  waiters.upgrade(lock_type, owners);
#endif
  release_latch();
#endif

  return rc;
}
