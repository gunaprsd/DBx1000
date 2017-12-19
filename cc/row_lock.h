// Copyright [2017] <Guna Prasaad>

#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"

#ifndef CC_ROW_LOCK_H_
#define CC_ROW_LOCK_H_



struct LockEntry {
  lock_t type;
  txn_man *txn;
  LockEntry *next;
  LockEntry *prev;

  static bool conflict_lock(lock_t l1, lock_t l2) {
    if(l1 == LOCK_NONE || l2 == LOCK_NONE)
      return false;
    else if(l1 == LOCK_EX || l2 == LOCK_EX)
      return true;
    else
      return false;
  }
};

struct Owners {
  void init() {
    count = 0;
    head = nullptr;
  }

  void push(LockEntry *en) {
    en->next = head;
    head = en;
    count++;
  }

  bool remove(txn_man *txn, LockEntry *&entry) {
    entry = head;
    LockEntry *prev_entry = nullptr;
    while (entry != nullptr) {
      if (entry->txn == txn) {
        if (prev_entry == nullptr) {
          assert(entry == head);
          head = entry->next;
        } else {
          prev_entry->next = entry->next;
        }
	count--;
        return true;
      } else {
        prev_entry = entry;
        entry = entry->next;
      }
    }
    return false;
  }

  uint32_t size() { return count; }

  bool can_wait(ts_t t) {
    auto entry = head;
    while (entry != nullptr) {
      if (t > entry->txn->get_ts()) {
        return false;
        break;
      }
      entry = entry->next;
    }
    return true;
  }

  void add_dependencies(LockEntry *entry, uint64_t *txnids, int &cursor) {
    auto en = head;
    while (en != nullptr) {
      if (LockEntry::conflict_lock(entry->type, en->type)) {
        txnids[cursor] = en->txn->get_txn_id();
        cursor++;
      }
      en = en->prev;
    }
  }

protected:
  uint32_t count;
  LockEntry *head;
};

struct Waiters {
public:
  void init() {
    count = 0;
    head = nullptr;
    tail = nullptr;
  }

  LockEntry *pop() {
    auto entry = head;
    head = head->next;
    if (head == nullptr) {
      tail = nullptr;
    }
    count--;
    return entry;
  }

  void push(LockEntry *entry) {
    if (count > 0) {
      entry->prev = tail;
      tail->next = entry;
      tail = entry;
    } else {
      assert(head == nullptr);
      head = entry;
      tail = entry;
    }
    count++;
  }

  void remove(txn_man *txn, LockEntry *&entry) {
    entry = head;
    while (entry != nullptr) {
      if (entry->txn != txn) {
        entry = entry->next;
      } else {
        break;
      }
    }
    assert(entry != nullptr);
    remove_entry(entry);
  }

  void upgrade(lock_t &lock_type, Owners &owners) {
    LockEntry *entry = nullptr;
    while (head != nullptr) {
      if (!LockEntry::conflict_lock(lock_type, head->type)) {
        entry = pop();
        owners.push(entry);
        lock_type = entry->type;
        assert(!entry->txn->lock_ready);
        entry->txn->lock_ready = true;
      } else {
        break;
      }
    }
    assert(count == 0 ? head == nullptr : true);
  }

  void insert(LockEntry *entry) {
    if (count == 0) {
      insert_singleton_entry(entry);
    } else {
      // find the previous and after entry for the txn
      LockEntry *after_entry = head;
      LockEntry *prev_entry = nullptr;
      while (after_entry != nullptr) {
        if (entry->txn->get_ts() > after_entry->txn->get_ts()) {
          prev_entry = after_entry;
          after_entry = after_entry->next;
        } else {
          break;
        }
      }

      insert_entry_before(entry, prev_entry);
    }
  }

  uint32_t size() { return count; }

  void add_dependencies(LockEntry *entry, uint64_t *txnids, int &cursor) {
    assert(tail == entry);
    auto iter = tail->prev;
    while (iter != nullptr) {
      if (LockEntry::conflict_lock(entry->type, iter->type)) {
        txnids[cursor] = iter->txn->get_txn_id();
        cursor++;
        iter = iter->prev;
      }
    }
  }

protected:
  void insert_singleton_entry(LockEntry *entry) {
    assert(head == nullptr);
    assert(tail == nullptr);
    entry->next = nullptr;
    entry->prev = nullptr;
    head = entry;
    tail = entry;
    count++;
  }

  void insert_entry_before(LockEntry *entry, LockEntry *before) {
    entry->prev = nullptr;
    entry->next = nullptr;

    if (before == nullptr) {
      head->prev = entry;
      entry->next = head;
      head = entry;
    } else if (before->next == nullptr) {
      tail->next = entry;
      entry->prev = tail;
      tail = entry;
    } else {
      entry->prev = before;
      entry->next = before->next;
      before->next = entry;
      entry->next->prev = entry;
    }
    count++;
  }

  void remove_entry(LockEntry *entry) {
    if (entry->prev == nullptr && entry->next == nullptr) {
      head = nullptr;
      tail = nullptr;
    } else if (entry->prev == nullptr) {
      head = entry->next;
      head->prev = nullptr;
    } else if (entry->next == nullptr) {
      tail = entry->prev;
      tail->next = nullptr;
    } else {
      entry->prev->next = entry->next;
      entry->next->prev = entry->prev;
    }
    count--;
  }

  uint32_t count;
  LockEntry *head;
  LockEntry *tail;
};

class RowLock {
public:
  void init(row_t *row);

  RC lock_get(lock_t type, txn_man *txn);

  RC lock_get(lock_t type, txn_man *txn, uint64_t *&txnids, int &txncnt);

  RC lock_release(txn_man *txn);

private:
  pthread_mutex_t *latch;

	LockEntry *get_entry() {
    LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry),
                                                        _row->get_part_id());
    return entry;
  }

  void return_entry(LockEntry *entry) {
    mem_allocator.free(entry, sizeof(LockEntry));
  }

  void get_latch() {
    if (g_central_man) {
      glob_manager->lock_row(_row);
    } else {
      pthread_mutex_lock(latch);
    }
  }

  void release_latch() {
    if (g_central_man) {
      glob_manager->release_row(_row);
    } else {
      pthread_mutex_unlock(latch);
    }
  }

  row_t *_row;
  lock_t lock_type;

  // owners is a single linked list
  Owners owners;
  Waiters waiters;
};

#endif
