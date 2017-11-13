#pragma once 

#include "../system/Global.h"
#include "../system/Helper.h"
#include "AbstractIndex.h"

#ifndef __STORAGE_HASH_INDEX_H__
#define __STORAGE_HASH_INDEX_H__

/* Hash Table Implementation:
 ****************************
 * A simple hash table implementation designed as an array of buckets for each partition.
 * Each bucket is a linked-list of bucket-nodes. Concurrency in the linked-list is handled using
 * an exclusive lock on the bucket for inserting new nodes.
 */

struct BucketNode
{
			BucketNode	(Key key);
	void 	initialize	(Key key);

	/* Data Fields */
	Key 				key;
	Record * 		records;
	BucketNode * 	next;
};

struct Bucket
{
					Bucket			();
					~Bucket			();
	void 			initialize		();
	void 			insert_record	(Key key, Record * record, PartId part_id);
	void 			read_record		(Key key, Record * & record, const char * tname);

	/* Data Fields */
	BucketNode * 	first_node;
	uint64_t 		node_cnt;
	bool 			locked;
};

// TODO Hash index does not support partition yet.
class HashIndex  : public AbstractIndex
{
public:
	/* Constructor/Destructor */
					HashIndex	();
					~HashIndex	();

	/* Initializers */
	Status 			initialize	(uint64_t bucket_cnt, uint64_t part_cnt);
	Status 			initialize	(Table * table, uint64_t bucket_cnt, uint64_t part_cnt);

	/* Index Accessor Function */
	bool 			exists	(Key key) override;
	Status 			insert	(Key key, Record * item, PartId part_id = -1) override;
	Status	 		read		(Key key, Record * & item, PartId part_id = -1) override;
	Status	 		read		(Key key, Record * & item, PartId part_id = -1, ThreadId thd_id = 0) override;

private:
	void 			get_latch		(Bucket * bucket);
	void 			release_latch	(Bucket * bucket);
	uint64_t 		hash				(Key key);
	
	/* Data Fields */
	Bucket ** 			_buckets;
	uint64_t	 			_bucket_cnt;
	uint64_t 			_part_cnt;
	uint64_t 			_bucket_cnt_per_part;
};

#endif
