#include "database.h"
#include "catalog.h"
#include "global.h"
#include "helper.h"
#include "index_btree.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
#include <pthread.h>

void Database::initialize(uint64_t num_threads) { _num_threads = num_threads; }

RC Database::initialize_schema(string schema_file) {
  assert(sizeof(uint64_t) == 8);
  assert(sizeof(double) == 8);
  string line;
  ifstream fin(schema_file);
  Catalog *schema;
  while (getline(fin, line)) {
    if (line.compare(0, 6, "TABLE=") == 0) {
      string tname;
      tname = &line[6];
      schema = (Catalog *)_mm_malloc(sizeof(Catalog), CL_SIZE);
      getline(fin, line);
      int col_count = 0;
      // Read all fields for this table.
      vector<string> lines;
      while (line.length() > 1) {
        lines.push_back(line);
        getline(fin, line);
      }
      schema->init(tname.c_str(), lines.size());
      for (uint32_t i = 0; i < lines.size(); i++) {
        string line = lines[i];
        size_t pos = 0;
        string token;
        int elem_num = 0;
        int size = 0;
        string type;
        string name;
        while (line.length() != 0) {
          pos = line.find(",");
          if (pos == string::npos)
            pos = line.length();
          token = line.substr(0, pos);
          line.erase(0, pos + 1);
          switch (elem_num) {
          case 0:
            size = atoi(token.c_str());
            break;
          case 1:
            type = token;
            break;
          case 2:
            name = token;
            break;
          default:
            assert(false);
          }
          elem_num++;
        }
        assert(elem_num == 3);
        schema->add_col((char *)name.c_str(), size, (char *)type.c_str());
        col_count++;
      }
      table_t *cur_tab = (table_t *)_mm_malloc(sizeof(table_t), CL_SIZE);
      cur_tab->init(schema);
      tables[tname] = cur_tab;
    } else if (!line.compare(0, 6, "INDEX=")) {
      string iname;
      iname = &line[6];
      getline(fin, line);

      vector<string> items;
      string token;
      size_t pos;
      while (line.length() != 0) {
        pos = line.find(",");
        if (pos == string::npos)
          pos = line.length();
        token = line.substr(0, pos);
        items.push_back(token);
        line.erase(0, pos + 1);
      }

      string tname(items[0]);
      INDEX *index = (INDEX *)_mm_malloc(sizeof(INDEX), 64);
      new (index) INDEX();

      int part_cnt = 1;
      uint64_t size;
      if (tname == "MAIN_TABLE") {
        size = FLAGS_ycsb_table_size * 2;
        part_cnt = FLAGS_ycsb_num_partitions;
      } else {
        if (tname == "ITEM") {
          part_cnt = 1;
        } else {
          part_cnt = FLAGS_tpcc_num_wh;
        }
        size = static_cast<uint64_t>(stoi(items[1]) * part_cnt);
      }

#if INDEX_STRUCT == IDX_HASH
	    index->init(part_cnt, tables[tname], size);
#else
      index->init(part_cnt, tables[tname]);
#endif
      indexes[iname] = index;
    }
  }
  fin.close();
  return RCOK;
}

void Database::load() {
  pthread_t threads[_num_threads];
  ThreadLocalData data[_num_threads];

  uint64_t start_time = get_server_clock();
  for (uint32_t i = 0; i < _num_threads; i++) {
    data[i].fields[0] = (uint64_t)this;
    data[i].fields[1] = (uint64_t)i;
    pthread_create(&threads[i], NULL, run_helper, (void *)&data[i]);
  }

  for (uint32_t i = 0; i < _num_threads; i++) {
    pthread_join(threads[i], NULL);
  }

  uint64_t end_time = get_server_clock();
  double duration =
      ((double)(end_time - start_time)) / 1000.0 / 1000.0 / 1000.0;
  printf("Database Loading Completed in %lf secs using %lu threads\n", duration,
         _num_threads);
}

void Database::index_insert(string index_name, uint64_t key, row_t *row) {
  assert(false);
  INDEX *index = (INDEX *)indexes[index_name];
  index_insert(index, key, row);
}

void Database::index_insert(INDEX *index, uint64_t key, row_t *row,
                            int64_t part_id) {
  uint64_t pid = part_id;
  if (part_id == -1)
    pid = get_part_id(row);
  itemid_t *m_item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t), pid);
  m_item->init();
  m_item->type = DT_row;
  m_item->location = row;
  m_item->valid = true;

  assert(index->index_insert(key, m_item, pid) == RCOK);
}

void *Database::run_helper(void *ptr) {
  ThreadLocalData *data = (ThreadLocalData *)ptr;
  Database *database = (Database *)data->fields[0];
  uint32_t thread_id = (uint32_t)((uint64_t)data->fields[1]);
  set_affinity(thread_id);
  database->load_tables(thread_id);
  return NULL;
}
