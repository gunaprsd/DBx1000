#include "global.h"	
#include "HashIndex.h"
#include "mem_alloc.h"
#include "Table.h"

/**********************************
 * BucketNode Definitions
 **********************************/

BucketNode::BucketNode(Key key)
{
	initialize(key);
}

void BucketNode::initialize(Key key)
{
	this->key = key;
	next = NULL;
	records = NULL;
}


/***********************************
 * BucketHeader Definitions
 ***********************************/

Bucket::Bucket()
{
	initialize();
}

Bucket::~Bucket() {}

void Bucket::initialize()
{
	node_cnt = 0;
	locked = false;
	first_node = NULL;
}

inline void Bucket::insert_record(Key key, Record * record, PartId part_id)
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key) break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}

	if (cur_node == NULL) {
		BucketNode * new_node = (BucketNode *) mem_allocator.alloc(sizeof(BucketNode), part_id );
		new_node->initialize(key);
		new_node->records = record;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		record->next = cur_node->records;
		cur_node->records = record;
	}
}

inline void Bucket::read_record(Key key, Record * & record, const char * tname)
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key) break;
		cur_node = cur_node->next;
	}
	M_ASSERT(cur_node->key == key, "Key does not exist!");
	record = cur_node->records;
}


/********************************
 * HashIndex Definitions
 ********************************/

HashIndex::HashIndex	()
{
	_buckets = NULL;
	_bucket_cnt = 0;
	_part_cnt = 0;
	_bucket_cnt_per_part = 0;
}

HashIndex::~HashIndex()
{
	for (int i = 0; i < _part_cnt; i++) {
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++) {
			_buckets[i][n]->~Bucket();
		}
		_mm_free(_buckets[i]);
	}
}

Status HashIndex::initialize(uint64_t bucket_cnt, uint64_t part_cnt)
{
	_bucket_cnt = bucket_cnt;
	_part_cnt = part_cnt;
	_bucket_cnt_per_part = _bucket_cnt / _part_cnt;
	_buckets = new Bucket * [_part_cnt];
	for (int i = 0; i < _part_cnt; i++) {
		_buckets[i] = (Bucket *) _mm_malloc(sizeof(Bucket) * _bucket_cnt_per_part, 64);
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++) {
			_buckets[i][n].initialize();
		}
	}
	return OK;
}

Status HashIndex::initialize(Table * table, uint64_t bucket_cnt, uint64_t part_cnt)
{
	this->table = table;
	initialize(bucket_cnt, part_cnt);
	return OK;
}

inline bool HashIndex::exists(Key key)
{
	assert(false);
	return false;
}
	
inline Status HashIndex::insert(Key key, Record * record, PartId part_id)
{
	uint64_t bucket_index = hash(key);
	assert(bucket_index < _bucket_cnt_per_part);
	Bucket * bucket = &_buckets[part_id][bucket_index];
	get_latch(bucket);
	bucket->insert_record(key, record, part_id);
	release_latch(bucket);
	return OK;
}

inline Status HashIndex::read(Key key, Record * & record, PartId part_id)
{
	uint64_t bucket_index = hash(key);
	assert(bucket_index < _bucket_cnt_per_part);
	Bucket * bucket = &_buckets[part_id][bucket_index];
	//get_latch(cur_bkt);
	bucket->read_record(key, record, table->get_table_name());
	//release_latch(cur_bkt);
	return OK;
}

inline Status HashIndex::read(Key key, Record * &item, PartId part_id, ThreadId thd_id)
{
	uint64_t bucket_index = hash(key);
	assert(bucket_index < _bucket_cnt_per_part);
	Bucket * bucket = &_buckets[part_id][bucket_index];
	//get_latch(bucket);
	bucket->read_record(key, item, table->get_table_name());
	//release_latch(bucket);
	return OK;
}

inline void HashIndex::get_latch(Bucket * bucket)
{
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

inline void HashIndex::release_latch(Bucket * bucket)
{
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

inline uint64_t HashIndex::hash(Key key)
{
	return key % _bucket_cnt_per_part;
}
