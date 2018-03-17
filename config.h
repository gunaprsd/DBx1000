

#ifndef _CONFIG_H_
#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define MAX_NUM_ACCESSES 30
#define MAX_NUM_CORES 30
#define MAX_NUM_TABLES 6
#define NOGRAPHITE 1
#define THREAD_CNT 4
#define PART_CNT 2
// each transaction only accesses 1 virtual compute_partitions. But the lock/ts
// manager and index are not aware of such partitioning. VIRTUAL_PART_CNT
// describes the request distribution and is only used to generate queries. For
// HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT 2
#define PAGE_SIZE 4096
#define CL_SIZE 64
// CPU_FREQ is used to get accurate timing info
#define CPU_FREQ 1.28 // in GHz/s

// # of transactions to run for warmup
#define WARMUP 0
// YCSB or TPCC
//#define WORKLOAD						YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE true
#define TIME_ENABLE true

#define MEM_ALLIGN 8

// [THREAD_ALLOC]
#define THREAD_ALLOC false
#define THREAD_ARENA_SIZE (1UL << 22)
#define MEM_PAD true

// [PART_ALLOC]
#define PART_ALLOC false
#define MEM_SIZE (1UL << 30)
#define NO_FREE false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HEKATON, HSTORE, OCC, VLL,
// TICTOC, SILO
// TODO TIMESTAMP does not work at this moment
#define CC_ALG NONE
#define ISOLATION_LEVEL SERIALIZABLE

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER false
// transaction roll back changes after abort
#define ROLL_BACK true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN false
#define BUCKET_CNT 31
#define ABORT_PENALTY 100000
#define ABORT_BUFFER_SIZE 10
#define ABORT_BUFFER_ENABLE true
// [ INDEX ]
#define ENABLE_LATCH false
#define CENTRAL_INDEX false
#define CENTRAL_MANAGER false
#define INDEX_STRUCT IDX_HASH
#define BTREE_ORDER 16

// [DL_DETECT]
#define DL_LOOP_DETECT 1000 // 100 us
#define DL_LOOP_TRIAL 100   // 1 us
#define NO_DL KEY_ORDER
#define TIMEOUT 1000000 // 1ms

// [TIMESTAMP]
#define TS_TWR false
#define TS_ALLOC TS_CAS
#define TS_BATCH_ALLOC false
#define TS_BATCH_NUM 1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
//#define HIS_RECYCLE_LEN				10
//#define MAX_PRE_REQ					1024
//#define MAX_READ_REQ				1024
#define MIN_TS_INTVL 5000000 // 5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET 10
#define PER_ROW_VALID true
// [TICTOC]
#define WRITE_COPY_FORM "data" // ptr or data
#define TICTOC_MV false
#define WR_VALIDATION_SEPARATE true
#define WRITE_PERMISSION_LOCK false
#define ATOMIC_TIMESTAMP "false"
// [TICTOC, SILO]
#define VALIDATION_LOCK_WAIT false // no-wait or waiting
#define PRE_ABORT true
#define ATOMIC_WORD true
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single compute_partitions transactions.
#define HSTORE_LOCAL_TS false
// [VLL]
#define TXN_QUEUE_SIZE_LIMIT THREAD_CNT

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND false
#define LOG_REDO false
#define LOG_BATCH_TIME 10 // in ms

/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN 64
#define QUERY_INTVL 1UL
#define MAX_TXN_PER_PART 32 * 1024
#define FIRST_PART_LOCAL false
#define MAX_TUPLE_SIZE 1024 // in bytes

#define INIT_PARALLELISM 64

// === [EXPERIMENT & YCSB]
#define YCSB_TABLE_SIZE (10 * 1024 * 1024)
#define YCSB_READ_PERCENT 0.9


// ==== [EXPERIMENT] ===
#define CONTENTION_PERC 1.0
#define POS_IN_TXN 1

// ==== [YCSB] ====
#define YCSB_PART_COUNT 4
#define YCSB_ZIPF_THETA 0
#define YCSB_MULTIPART_PERCENT 0
#define YCSB_NUM_REQUESTS 16
#define YCSB_NUM_LOCAL 1
#define YCSB_NUM_REMOTE 1
#define YCSB_DO_COMPUTE false
#define YCSB_COMPUTE_COST 100
#define YCSB_REMOTE_PERCENT 0.3
#define YCSB_KEY_ORDER false
#define REQ_PER_QUERY 16
#define FIELD_PER_TUPLE 10


// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL false
#define TPCC_WH_UPDATE true
#define TPCC_NUM_WH 4
#define TPCC_MAX_NUM_ORDERS 10
#define TPCC_PERC_PAYMENT 0.5
#define TPCC_DIST_PER_WH 10
#define TPCC_BY_LAST_NAME_PERC 0
#define TPCC_REMOTE_PAYMENT_PERC 0
#define TPCC_NUM_ORDERS_RANDOM true

#if TPCC_SMALL
#define TPCC_MAX_ITEMS 10000;
#define TPCC_CUST_PER_DIST 2000;
#else
#define TPCC_MAX_ITEMS 100000
#define TPCC_CUST_PER_DIST 3000
#endif

#define FIRSTNAME_MINLEN 8
#define FIRSTNAME_LEN 16
#define LASTNAME_LEN 16



/***********************************************/
// TODO centralized CC management.
/***********************************************/
#define MAX_LOCK_CNT (20 * THREAD_CNT)
#define TSTAB_SIZE (50 * THREAD_CNT)
#define TSTAB_FREE TSTAB_SIZE
#define TSREQ_FREE (4 * TSTAB_FREE)
#define MVHIS_FREE (4 * TSTAB_FREE)
#define SPIN false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL true
enum TestCases { READ_WRITE, CONFLICT };
extern TestCases g_test_case;
/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB true
#define IDX_VERB false
#define VERB_ALLOC true

#define DEBUG_LOCK false
#define DEBUG_TIMESTAMP false
#define DEBUG_SYNTH false
#define DEBUG_ASSERT false
#define DEBUG_CC false // true

/***********************************************/
// Constant
/***********************************************/

// Index Structure
#define IDX_HASH 1
#define IDX_BTREE 2

// Workload
#define YCSB 1
#define TPCC 2

// Concurrency Control Algorithm
#define NO_WAIT 1
#define WAIT_DIE 2
#define DL_DETECT 3
#define TIMESTAMP 4
#define MVCC 5
#define HSTORE 6
#define OCC 7
#define TICTOC 8
#define SILO 9
#define VLL 10
#define HEKATON 11
#define NONE 12

// Isolation Levels
#define SERIALIZABLE 1
#define SNAPSHOT 2
#define REPEATABLE_READ 3

// TIMESTAMP allocation method.
#define TS_MUTEX 1
#define TS_CAS 2
#define TS_HW 3
#define TS_CLOCK 4

#define MAX_NODES_FOR_CLUSTERING (16 * 1024)

#endif
