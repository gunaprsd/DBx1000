CMAKE_MINIMUM_REQUIRED(VERSION 3.5.1)
PROJECT(DBx1000)
SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "-std=c++11 -Werror -Wall -D CC_ALG=NO_WAIT" CACHE INTERNAL "compiler options" FORCE)
SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET(CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)
IF(NOT CMAKE_BUILD_TYPE)
    SET (CMAKE_BUILD_TYPE  "Release")
ENDIF()

SET (SOURCES_LIST tpcc/tpcc_helper.h
        tpcc/tpcc.h
        tpcc/tpcc_const.h
        ycsb/ycsb.h
        cc/occ.h
        cc/plock.h
        cc/vll.h
        cc/row_tictoc.h
        cc/row_mvcc.h
        cc/row_vll.h
        cc/row_hekaton.h
        cc/row_silo.h
        cc/row_occ.h
        cc/row_lock.h
        cc/dl_detect.h
        cc/row_ts.h
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
        workload/data_partitioner.h
        workload/graph_partitioner.h
        workload/partitioner.h
        workload/generator.h
        cc/row_ts.cpp
        cc/vll.cpp
        cc/row_hekaton.cpp
        cc/occ.cpp
        cc/row_mvcc.cpp
        cc/dl_detect.cpp
        cc/row_vll.cpp
        cc/row_tictoc.cpp
        cc/row_occ.cpp
        cc/plock.cpp
        cc/row_lock.cpp
        cc/silo.cpp
        cc/hekaton.cpp
        cc/tictoc.cpp
        cc/row_silo.cpp
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
        workload/data_partitioner.cpp
        workload/generator.cpp
        workload/graph_partitioner.cpp
        workload/partitioner.cpp)

LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/benchmarks/ycsb)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/benchmarks/tpcc)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/cc)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/storage)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/workload)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/system)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib/include)

ADD_EXECUTABLE(DBx1000 ${SOURCES_LIST})
