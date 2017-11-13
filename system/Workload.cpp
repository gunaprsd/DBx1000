#include "BTreeIndex.h"
#include "HashIndex.h"
#include "Table.h"
#include "Row.h"
#include "Catalog.h"
#include "Workload.h"
#include "Allocator.h"
#include "Global.h"
#include "Helper.h"

Workload::Workload()
{
	sim_done = false;
}

Workload::~Workload() {}

Status Workload::initialize()
{
	sim_done = false;
	return OK;
}

Status Workload::initialize_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);

	string line;
	ifstream fin(schema_file);
    Catalog * schema;

    while (getline(fin, line)) {

    		if (line.compare(0, 6, "TABLE=") == 0) {
    			//Obtain table name
    			string tname = & line[6];

			// Read all fields for this table.
			vector<string> lines;
			getline(fin, line);
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}

			//Initialize schema
			schema = (Catalog *) _mm_malloc(sizeof(Catalog), CACHE_LINE_SIZE);
			schema->initialize( tname.c_str(), lines.size() );
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
					if (pos == string::npos) {
						pos = line.length();
					}
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
                schema->add_column(name.c_str(), type.c_str(), size);
			}

			//Create a Table and add to array
			Table * table = (Table *) _mm_malloc(sizeof(Table), CACHE_LINE_SIZE);
			table->initialize(schema);
			tables[tname] = table;

        } else if (!line.compare(0, 6, "INDEX=")) {
			string iname = & line[6];

			string token;
			size_t pos;
			vector<string> items;
			getline(fin, line);
			while (line.length() != 0) {
				pos = line.find(",");
				if (pos == string::npos) {
					pos = line.length();
				}
	    			token = line.substr(0, pos);
				items.push_back(token);
		    		line.erase(0, pos + 1);
			}
			
			string tname(items[0]);
			INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
			new(index) INDEX();
			uint64_t part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
			if (tname == "ITEM") {
				part_cnt = 1;
			}
#if INDEX_STRUCT == IDX_HASH
	#if WORKLOAD == YCSB
			index->initialize(tables[tname], g_synth_table_size * 2, part_cnt);
	#elif WORKLOAD == EXPERIMENT
			index->initialize(tables[tname], g_synth_table_size * 2, part_cnt);
	#elif WORKLOAD == TPCC
			assert(tables[tname] != NULL);
			index->initialize(tables[tname], stoi( items[1] ) * part_cnt, part_cnt);
	#endif
#else
			index->initialize(tables[tname], part_cnt);
#endif
			indexes[iname] = index;
		}
    }
	fin.close();
	return OK;
}

void Workload::insert_into_index(string index_name, uint64_t key, Row * row)
{
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	insert_into_index(index, key, row);
}

void Workload::insert_into_index(INDEX * index, Key key, Row * row, PartId part_id)
{
	uint64_t pid = part_id;
	if (part_id == UINT64_MAX) {
		pid = get_part_id(row);
	}

	Record * record = (Record *) mem_allocator.allocate( sizeof(Record), pid);
	record->initialize();
	record->type = DT_ROW;
	record->location = row;
	record->valid = true;
	Status status = index->insert(key, record, pid);
    assert(status == OK);
}


