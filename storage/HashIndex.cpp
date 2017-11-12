#include "global.h"	
#include "HashIndex.h"
#include "mem_alloc.h"
#include "Table.h"


		BucketNode::BucketNode	(KeyId key) {	initialize(key); };
	void 	BucketNode::initialize	(KeyId key)
	{
		this->key = key;
		next = NULL;
		records = NULL;
	}

Status HashIndex::initialize(uint64_t bucket_cnt, uint32_t part_cnt) {
	_bucket_cnt = bucket_cnt;
	_bucket_cnt_per_part = bucket_cnt / part_cnt;
	_buckets = new BucketHeader * [part_cnt];
	for (int i = 0; i < part_cnt; i++) {
		_buckets[i] = (BucketHeader *) _mm_malloc(sizeof(BucketHeader) * _bucket_cnt_per_part, 64);
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++)
			_buckets[i][n].initialize();
	}
	return OK;
}

Status HashIndex::initialize(uint32_t part_cnt, Table * table, uint64_t bucket_cnt) {
	initialize(bucket_cnt, part_cnt);
	this->table = table;
	return OK;
}

bool HashIndex::exists(KeyId key) {
	assert(false);
}

void 
HashIndex::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void 
HashIndex::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

	
Status HashIndex::insert(KeyId key, Record * item, PartId part_id) {
	Status rc = OK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

Status HashIndex::read(KeyId key, Record * &item, PartId part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	Status rc = OK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

Status HashIndex::read(KeyId key, Record * &item, PartId part_id, ThreadId thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	Status rc = OK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;
}

/************** BucketHeader Operations ******************/

void BucketHeader::initialize() {
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}

void BucketHeader::insert_item(KeyId key, Record * item, PartId part_id)
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {		
		BucketNode * new_node = (BucketNode *) 
			mem_allocator.alloc(sizeof(BucketNode), part_id );
		new_node->initialize(key);
		new_node->records = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		item->next = cur_node->records;
		cur_node->records = item;
	}
}

void BucketHeader::read_item(KeyId key, Record * &item, const char * tname)
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
	M_ASSERT(cur_node->key == key, "Key does not exist!");
	item = cur_node->records;
}
