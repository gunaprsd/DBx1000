PROJECT(DBx1000)
CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
SET(CMAKE_C_COMPILER /usr/bin/cc)
SET(CMAKE_C_COMPILER /usr/bin/c++)
SET(CMAKE_CXX_STANDARD 11)

ADD_DEFINITIONS(-DNOGRAPHITE=1)
SET (CMAKE_CXX_FLAGS "-Werror -Wall -std=c++0x" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_BUILD_TYPE "Release")
IF (NOT CMAKE_BUILD_TYPE)
    SET (CMAKE_BUILD_TYPE "Debug")
ENDIF ()

ADD_SUBDIRECTORY(gflags)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/ycsb)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/tpcc)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/cc)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/storage)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/system)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/workload)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/scheduler)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/utils)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib/include)

SET(SRC_LIST cc/occ.h
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
        config-std.h
        config.h
        lib/include/metis.h
        lib/include/parmetis.h
        storage/index_hash.h
        storage/index_base.h
        storage/catalog.h
        storage/table.h
        storage/index_btree.h
        storage/row.h
        helper.h
        system/query.h
        global.h
        system/manager.h
        system/txn.h
        scheduler/thread.h
        system/database.h
        utils/stats.h
        utils/mem_alloc.h
        tpcc/tpcc_utility.h
        tpcc/tpcc.h
        tpcc/tpcc_database.h
        tpcc/tpcc_const.h
        tpcc/tpcc_workload.h
        utils/distributions.h
        workload/graph_partitioner.h
        workload/generator.h
        ycsb/ycsb_workload.h
        ycsb/ycsb.h
        ycsb/ycsb_database.h
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
        storage/index_hash.cpp
        storage/table.cpp
        storage/row.cpp
        storage/index_btree.cpp
        storage/catalog.cpp
        utils/stats.cpp
        system/txn.cpp
        utils/mem_alloc.cpp
        helper.cpp
        system/manager.cpp
        system/database.cpp
        global.cpp
        parser.cpp
        main.cpp
        tpcc/tpcc_database.cpp
        tpcc/tpcc_workload.cpp
        tpcc/tpcc_utility.cpp
        workload/graph_partitioner.cpp
        ycsb/ycsb.cpp
        ycsb/ycsb_workload.cpp
        ycsb/ycsb_database.cpp
        workload/partitioner.h
        workload/loader.h
        ycsb/ycsb.cpp
        scheduler/offline_scheduler.h
        parser.h
        workload/partitioner_helper.h
        workload/partitioner.cpp
        scheduler/scheduler.h
        scheduler/abort_buffer.h
        scheduler/scheduler_tree.h
        scheduler/queues.h scheduler/scheduler_tree_v1.h scheduler/online_batch_scheduler.h scheduler/online_batch_thread.h)
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR})
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
ADD_EXECUTABLE(rundb ${SRC_LIST})
TARGET_LINK_LIBRARIES(rundb rt pthread metis -ltbb)
TARGET_LINK_LIBRARIES(rundb gflags)
