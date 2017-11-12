#include "global.h"
#include "helper.h"
#include "wl.h"

#include "../storage/BTreeIndex.h"
#include "../storage/HashIndex.h"
#include "../storage/Table.h"
#include "Row.h"
#include "Catalog.h"
#include "mem_alloc.h"

Status Workload::init() {
	sim_done = false;
	return OK;
}

Status Workload::init_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	string line;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname;
			tname = &line[6];
			schema = (Catalog *) _mm_malloc(sizeof(Catalog), CACHE_LINE_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->initialize( tname.c_str(), lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
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
					case 0: size = atoi(token.c_str()); break;
					case 1: type = token; break;
					case 2: name = token; break;
					default: assert(false);
					}
					elem_num ++;
				}
				assert(elem_num == 3);
                schema->add_column((char *)name.c_str(), size, (char *)type.c_str());
				col_count ++;
			}
			Table * cur_tab = (Table *) _mm_malloc(sizeof(Table), CACHE_LINE_SIZE);
			cur_tab->initialize(schema);
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
			INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
			new(index) INDEX();
			int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
			if (tname == "ITEM") {
				part_cnt = 1;
			}
#if INDEX_STRUCT == IDX_HASH
	#if WORKLOAD == YCSB
			index->initialize(part_cnt, tables[tname], g_synth_table_size * 2);
	#elif WORKLOAD == EXPERIMENT
			index->initialize(part_cnt, tables[tname], g_synth_table_size * 2);
	#elif WORKLOAD == TPCC
			assert(tables[tname] != NULL);
			index->initialize(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
	#endif
#else
			index->initialize(part_cnt, tables[tname]);
#endif
			indexes[iname] = index;
		}
    }
	fin.close();
	return OK;
}



void Workload::index_insert(string index_name, uint64_t key, Row * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void Workload::index_insert(INDEX * index, uint64_t key, Row * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	Record * m_item =
		(Record *) mem_allocator.alloc( sizeof(Record), pid );
	m_item->initialize();
	m_item->type = DT_ROW;
	m_item->location = row;
	m_item->valid = true;

    assert( index->index_insert(key, m_item, pid) == OK );
}


