#pragma once
#ifndef __TYPES_H__
#define __TYPES_H__

#include <stdint.h>

enum Status { OK, Commit, Abort, WAIT, ERROR, FINISH};

/* Thread */
typedef uint64_t TxnId;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t; // row id
typedef uint64_t pgid_t; // page id



/* INDEX */
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
typedef uint64_t Key; // key id for index
typedef uint64_t PartId;
typedef uint32_t ColumnId;
typedef uint64_t RowId;
typedef uint64_t ThreadId;
typedef uint64_t (*func_ptr)(Key);	// part_id func_ptr(index_key);

/* general concurrency control */
enum AccessType {RD, WR, XP, SCAN};
/* LOCK */
enum LockType {LOCK_EX, LOCK_SH, LOCK_NONE };
/* TIMESTAMP */
enum TimestampType {R_REQ, W_REQ, P_REQ, XP_REQ};


#define MSG(str, args...) { \
	printf("[%s : %d] " str, __FILE__, __LINE__, args); } \
//	printf(args); }

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX		index_btree
#else  // IDX_HASH
#define INDEX		HashIndex
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 		18446744073709551615UL
#endif // UINT64_MAX

#endif