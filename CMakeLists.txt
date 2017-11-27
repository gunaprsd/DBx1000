cmake_minimum_required(VERSION 3.8)
project(DBx1000)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        benchmarks/ycsb.h
        benchmarks/ycsb_workload.cpp
        benchmarks/ycsb_workload.h
        benchmarks/ycsb_txn.cpp
        benchmarks/ycsb_database.cpp
        concurrency_control/dl_detect.cpp
        concurrency_control/dl_detect.h
        concurrency_control/hekaton.cpp
        concurrency_control/occ.cpp
        concurrency_control/occ.h
        concurrency_control/plock.cpp
        concurrency_control/plock.h
        concurrency_control/row_hekaton.cpp
        concurrency_control/row_hekaton.h
        concurrency_control/row_lock.cpp
        concurrency_control/row_lock.h
        concurrency_control/row_mvcc.cpp
        concurrency_control/row_mvcc.h
        concurrency_control/row_occ.cpp
        concurrency_control/row_occ.h
        concurrency_control/row_silo.cpp
        concurrency_control/row_silo.h
        concurrency_control/row_tictoc.cpp
        concurrency_control/row_tictoc.h
        concurrency_control/row_ts.cpp
        concurrency_control/row_ts.h
        concurrency_control/row_vll.cpp
        concurrency_control/row_vll.h
        concurrency_control/silo.cpp
        concurrency_control/tictoc.cpp
        concurrency_control/vll.cpp
        concurrency_control/vll.h
        storage/catalog.cpp
        storage/catalog.h
        storage/index_base.h
        storage/index_btree.cpp
        storage/index_btree.h
        storage/index_hash.cpp
        storage/index_hash.h
        storage/row.cpp
        storage/row.h
        storage/table.cpp
        storage/table.h
        system/global.cpp
        system/global.h
        system/helper.cpp
        system/helper.h
        system/manager.cpp
        system/manager.h
        system/mem_alloc.cpp
        system/mem_alloc.h
        system/parser.cpp
        system/stats.cpp
        system/stats.h
        system/txn.cpp
        system/txn.h
        system/query.h
        system/workload.h
        system/workload.cpp
        config-std.h
        config.cpp
        config.h)

include_directories(.)
include_directories(benchmarks)
include_directories(concurrency_control)
include_directories(storage)
include_directories(system)

add_executable(DBx1000 ${SOURCE_FILES})