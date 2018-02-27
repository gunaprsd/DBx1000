#include "ycsb_database.h"
#include "catalog.h"
#include "global.h"
#include "graph_partitioner.h"
#include "helper.h"
#include "index_hash.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "row_lock.h"
#include "row_mvcc.h"
#include "row_ts.h"
#include "table.h"
#include "ycsb_workload.h"
#include <vector>

void YCSBDatabase::initialize(uint64_t num_threads) {
  Database::initialize(num_threads);
  char *cpath = getenv("GRAPHITE_HOME");
  string path;
  if (cpath == NULL)
    path = "./ycsb/schema.txt";
  else {
    path = string(cpath);
    path += "/tests/apps/dbms/schema.txt";
  }

  Database::initialize_schema(path);
  the_table = tables["MAIN_TABLE"];
  the_index = indexes["MAIN_INDEX"];
  uint64_t size = AccessIterator<ycsb_params>::get_max_key();
  data_next_pointer = new uint64_t[size];
  for (uint64_t i = 0; i < size; i++) {
    data_next_pointer[i] = 0;
  }
}

uint64_t YCSBDatabase::key_to_part(uint64_t key) {
  return key % config.num_partitions;
}

txn_man *YCSBDatabase::get_txn_man(uint64_t thread_id) {
  auto txn_manager = new YCSBTransactionManager();
  txn_manager->initialize(this, thread_id);
  return txn_manager;
}

void YCSBDatabase::load_tables(uint64_t thread_id) {
  mem_allocator.register_thread(thread_id);
  load_main_table(thread_id);
  mem_allocator.unregister();
}

void YCSBDatabase::load_main_table(uint64_t tid) {
  RC rc;
  uint64_t slice_size = config.table_size / _num_threads;
  for (uint64_t key = slice_size * tid; key < slice_size * (tid + 1); key++) {
    row_t *new_row = NULL;
    uint64_t row_id;
    int part_id = key_to_part(key);
    rc = the_table->get_new_row(new_row, part_id, row_id);
    assert(rc == RCOK);
    uint64_t primary_key = key;
    new_row->set_primary_key(primary_key);
    new_row->set_value(0, &primary_key);
    Catalog *schema = the_table->get_schema();

    for (uint32_t fid = 0; fid < schema->get_field_cnt(); fid++) {
      char value[6] = "hello";
      new_row->set_value(fid, value);
    }

    itemid_t *m_item =
        (itemid_t *)mem_allocator.alloc(sizeof(itemid_t), part_id);
    assert(m_item != NULL);
    m_item->type = DT_row;
    m_item->location = new_row;
    m_item->valid = true;
    uint64_t idx_key = primary_key;

    rc = the_index->index_insert(idx_key, m_item, part_id);
    assert(rc == RCOK);
  }
}

YCSBDatabase::YCSBDatabase(const YCSBBenchmarkConfig &_config)
    : config(_config), the_index(nullptr), the_table(nullptr) {}

void YCSBTransactionManager::initialize(Database *database,
                                        uint64_t thread_id) {
  txn_man::initialize(database, thread_id);
  db = (YCSBDatabase *)database;
}

RC YCSBTransactionManager::run_txn(BaseQuery *query) {
  RC rc;
  ycsb_params *m_query = &((ycsb_query *)query)->params;
  YCSBDatabase *wl = (YCSBDatabase *)database;
  itemid_t *m_item = NULL;
  row_cnt = 0;

  for (uint32_t rid = 0; rid < m_query->request_cnt; rid++) {
    ycsb_request *req = &m_query->requests[rid];
    int part_id = wl->key_to_part(req->key);
    bool finish_req = false;
    uint32_t iteration = 0;
    while (!finish_req) {
      if (iteration == 0) {
        m_item = index_read(db->the_index, req->key, part_id);
      }
#if INDEX_STRUCT == IDX_BTREE
      else {
        the_index->index_next(get_thd_id(), m_item);
        if (m_item == NULL)
          break;
      }
#endif
      row_t *row = ((row_t *)m_item->location);
      row_t *row_local;
      access_t type = req->rtype;

      if(SELECTIVE_CC && req->cc_info == 0) {
        row_local = row;
      } else {
        row_local = get_row(row, type);
        if (row_local == NULL) {
          rc = Abort;
          goto final;
        }
      }
      assert(row_local != nullptr);

      char *data = row_local->get_data();
      if (db->config.do_compute) {
        float a = *(float *)data;
        for (auto i = 0u; i < db->config.compute_cost; i++) {
          a *= a;
        }
        *(float *)data = a;
      }

      if (m_query->request_cnt > 1) {
        if (req->rtype == RD || req->rtype == SCAN) {
          int fid = 0;
          // char *data = row_local->get_data();
          __attribute__((unused)) uint64_t fval =
              *(uint64_t *)(&data[fid * 10]);
        } else {
          assert(req->rtype == WR);
          int fid = 0;
          // char *data = row->get_data();
          *(uint64_t *)(&data[fid * 10]) = 0;
        }
      }

      iteration++;
      if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
        finish_req = true;
    }
  }
  rc = RCOK;

final:
  rc = finish(rc);
  return rc;
}
