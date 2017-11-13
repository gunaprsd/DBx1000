#ifndef __STORAGE_BTREE_H__
#define __STORAGE_BTREE_H__

#include "AbstractIndex.h"
#include "global.h"
#include "helper.h"


typedef struct BTreeNode
{
	// TODO bad hack!
   	void ** pointers; // for non-leaf nodes, point to bt_nodes
	bool is_leaf;
	Key * keys;
	BTreeNode * parent;
	UInt32 num_keys;
	BTreeNode * next;
	bool latch;
	pthread_mutex_t locked;
	latch_t latch_type;
	UInt32 share_cnt;
} BTreeNode;

struct glob_param {
	uint64_t part_id;
};

class BTreeIndex : public AbstractIndex {
public:
	/* Constructors and Destructors */
					BTreeIndex	();
					~BTreeIndex	();

	/* Initializers */
	Status			initialize	(uint64_t part_cnt);
	Status			initialize	(uint64_t part_cnt, Table * table);

	/* Index Accessors */
	bool 			exists		(Key key) 	override;
	Status 			insert		(Key key, Record * item, PartId part_id = -1) override;

	Status	 		read			(Key key, Record * &item, PartId part_id = -1) override;
	Status	 		read			(Key key, Record * &item, PartId part_id = -1, ThreadId thread_id = 0) override;
	Status	 		read			(Key key, Record * &item);
	Status 			next			(uint64_t thd_id, Record * &item, bool samekey = false);

private:
	// index structures may have part_cnt = 1 or PART_CNT.
	uint64_t 		part_cnt;
	Status			make_lf(uint64_t part_id, BTreeNode *& node);
	Status			make_nl(uint64_t part_id, BTreeNode *& node);
	Status		 	make_node(uint64_t part_id, BTreeNode *& node);
	
	Status 			start_new_tree(glob_param params, Key key, Record * item);
	Status 			find_leaf(glob_param params, Key key, idx_acc_t access_type, BTreeNode *& leaf, BTreeNode  *& last_ex);
	Status 			find_leaf(glob_param params, Key key, idx_acc_t access_type, BTreeNode *& leaf);
	Status			insert_into_leaf(glob_param params, BTreeNode * leaf, Key key, Record * item);
	// handle split
	Status 			split_lf_insert(glob_param params, BTreeNode * leaf, Key key, Record * item);
	Status 			split_nl_insert(glob_param params, BTreeNode * node, UInt32 left_index, Key key, BTreeNode * right);
	Status 			insert_into_parent(glob_param params, BTreeNode * left, Key key, BTreeNode * right);
	Status 			insert_into_new_root(glob_param params, BTreeNode * left, Key key, BTreeNode * right);

	int				leaf_has_key(BTreeNode * leaf, Key key);
	
	UInt32 			cut(UInt32 length);
	UInt32	 		order; // # of keys in a node(for both leaf and non-leaf)
	BTreeNode ** 		roots; // each partition has a different root
	BTreeNode *   		find_root(uint64_t part_id);

	bool 			latch_node(BTreeNode * node, latch_t latch_type);
	latch_t			release_latch(BTreeNode * node);
	Status		 	upgrade_latch(BTreeNode * node);
	Status 			cleanup(BTreeNode * node, BTreeNode * last_ex);

	// the leaf and the idx within the leaf that the thread last accessed.
	BTreeNode *** cur_leaf_per_thd;
	UInt32 ** 		cur_idx_per_thd;
};

#endif
