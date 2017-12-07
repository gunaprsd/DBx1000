CMAKE_MINIMUM_REQUIRED(VERSION 3.5.1)
PROJECT(DBx1000)
SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "-std=c++11 -Werror -Wall -D CC_ALG=NO_WAIT" CACHE INTERNAL "compiler options" FORCE)
SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET(CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)
IF(NOT CMAKE_BUILD_TYPE)
    SET (CMAKE_BUILD_TYPE  "Release")
ENDIF()

SET (SOURCES_LIST benchmarks/tpcc/tpcc_helper.h
        benchmarks/tpcc/tpcc.h
        benchmarks/tpcc/tpcc_const.h
        benchmarks/ycsb/ycsb.h
        concurrency_control/occ.h
        concurrency_control/plock.h
        concurrency_control/vll.h
        concurrency_control/row_tictoc.h
        concurrency_control/row_mvcc.h
        concurrency_control/row_vll.h
        concurrency_control/row_hekaton.h
        concurrency_control/row_silo.h
        concurrency_control/row_occ.h
        concurrency_control/row_lock.h
        concurrency_control/dl_detect.h
        concurrency_control/row_ts.h
        config.h
        lib/include/metis.h
        storage/index_hash.h
        storage/index_base.h
        storage/catalog.h
        storage/table.h
        storage/index_btree.h
        storage/row.h
        system/helper.h
        system/query.h
        system/global.h
        system/manager.h
        system/txn.h
        system/thread_queue.h
        system/thread.h
        system/database.h
        system/stats.h
        system/mem_alloc.h
        workload/graph_partitioner.h
        workload/partitioner.h
        workload/generator.h
        concurrency_control/row_ts.cpp
        concurrency_control/vll.cpp
        concurrency_control/row_hekaton.cpp
        concurrency_control/occ.cpp
        concurrency_control/row_mvcc.cpp
        concurrency_control/dl_detect.cpp
        concurrency_control/row_vll.cpp
        concurrency_control/row_tictoc.cpp
        concurrency_control/row_occ.cpp
        concurrency_control/plock.cpp
        concurrency_control/row_lock.cpp
        concurrency_control/silo.cpp
        concurrency_control/hekaton.cpp
        concurrency_control/tictoc.cpp
        concurrency_control/row_silo.cpp
        config.cpp
        storage/index_hash.cpp
        storage/table.cpp
        storage/row.cpp
        storage/index_btree.cpp
        storage/catalog.cpp
        system/thread.cpp
        system/stats.cpp
        system/txn.cpp
        system/mem_alloc.cpp
        system/helper.cpp
        system/manager.cpp
        system/database.cpp
        system/global.cpp
        system/parser.cpp
        system/main.cpp
        workload/generator.cpp
        workload/graph_partitioner.cpp
        workload/partitioner.cpp)

LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/benchmarks/ycsb)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/benchmarks/tpcc)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/concurrency_control)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/storage)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/workload)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/system)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib/include)

ADD_EXECUTABLE(DBx1000 ${SOURCES_LIST})
