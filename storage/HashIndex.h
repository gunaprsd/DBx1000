#pragma once 

#include "AbstractIndex.h"
#include "global.h"
#include "helper.h"

class BucketNode {
public: 
			BucketNode	(KeyId key);
	void 	initialize	(KeyId key);

	/* Data Fields */
	KeyId 			key;
	Record * 		records;
	BucketNode * 	next;
};

class BucketHeader {
public:
	void 			initialize	();
	void 			insert_item	(KeyId key, Record * item, PartId part_id);
	void 			read_item	(KeyId key, Record * & item, const char * tname);

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
	Status 			initialize	(uint64_t bucket_cnt, uint32_t part_cnt);
	Status 			initialize	(uint32_t part_cnt, Table * table, uint64_t bucket_cnt);

	/* Index Accessor Function */
	bool 			exists	(KeyId key);
	Status 			insert	(KeyId key, Record * item, PartId part_id=-1);
	Status	 		read		(KeyId key, Record * &item, PartId part_id=-1);
	Status	 		read		(KeyId key, Record * &item, PartId part_id=-1, ThreadId thd_id=0);

private:
	void 			get_latch		(BucketHeader * bucket);
	void 			release_latch	(BucketHeader * bucket);
	uint64_t 		hash				(KeyId key) {	return key % _bucket_cnt_per_part; }
	
	/* Data Fields */
	BucketHeader ** 		_buckets;
	uint64_t	 			_bucket_cnt;
	uint64_t 			_bucket_cnt_per_part;
};
