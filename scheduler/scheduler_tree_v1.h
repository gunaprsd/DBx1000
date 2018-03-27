#ifndef DBX1000_SCHEDULER_TREE_V1_H
#define DBX1000_SCHEDULER_TREE_V1_H

#include "database.h"
#include "loader.h"
#include "query.h"
#include "queues.h"
#include "scheduler.h"
#include <atomic>
#include <unistd.h>
#include <unordered_set>
using namespace std;

template <typename T>
class SchedulerTreeV1 : public ITransactionQueue<T> {
	typedef Query<T> Node;
	int32_t num_threads;
	SharedQueryQueue<T> input_queue;
	Node **data_nodes;
	Node **active_nodes;

public:
	SchedulerTreeV1(int32_t num_threads) : num_threads(num_threads), input_queue() {
		// create an array to store individual data leaf nodes
		uint64_t size = AccessIterator<T>::get_max_key();
		data_nodes = new Node *[size];
		for (uint64_t i = 0; i < size; i++) {
			data_nodes[i] = nullptr;
			// data_nodes[i]->is_data_node = true;
		}

		active_nodes = new Node *[num_threads];
		for (int32_t i = 0; i < num_threads; i++) {
			active_nodes[i] = nullptr;
		}
	}
	bool next(int32_t thread_id, Query<T> *&txn) override {
		// get a new position node in the tree, if currently null
		while (true) {
			if (active_nodes[thread_id] == nullptr) {
				if (!input_queue.try_pop(active_nodes[thread_id])) {
					return false;
				}
			}
			if (try_dequeue(active_nodes[thread_id], txn)) {
				// try to dequeue something - if successful return
				return true;
			} else {
				// get a new node in next iteration
				active_nodes[thread_id] = nullptr;
			}
		}
	}
	void add(Query<T> *txn, int32_t thread_id = -1) {
		ReadWriteSet rwset;
		txn->obtain_rw_set(&rwset);
		unordered_set<Node *> root_nodes;
		int64_t num_active_children = 0;
		int64_t num_data_children = 0;
		int32_t max_size = 0;
		Node* max_size_node = nullptr;
		for (uint32_t i = 0; i < rwset.num_accesses; i++) {
			auto key = rwset.accesses[i].key;
			auto data_node = data_nodes[key];
			auto root_node = find_root(data_node);
			assert(root_node == find_root(root_node));
			if (root_node != nullptr) {
				if (root_nodes.find(root_node) == root_nodes.end()) {
					root_nodes.insert(root_node);
					num_active_children++;
					if(max_size < root_node->size) {
						max_size = root_node->size;
						max_size_node = root_node;
					}
				}
			} else {
				num_data_children++;
			}
		}

		Node* real_root_node = nullptr;
		if(num_active_children >= 1) {
			real_root_node = max_size_node;
			for(auto child_node : root_nodes) {
				if(child_node != max_size_node) {
					child_node->parent = real_root_node;
					merge(real_root_node, child_node);
				}
			}
			enqueue(real_root_node, txn);
		} else {
			real_root_node = txn;
			real_root_node->parent = nullptr;
			real_root_node->next = nullptr;
			real_root_node->head = nullptr;
			real_root_node->tail = nullptr;
			real_root_node->size = 0;
			enqueue(real_root_node, txn);
			input_queue.push(real_root_node);
		}

		for (uint32_t i = 0; i < rwset.num_accesses; i++) {
			auto key = rwset.accesses[i].key;
			data_nodes[key] = real_root_node;
		}
	}
protected:
	void merge(Node* node1, Node* node2) {
		if(node1->head == nullptr) {
			node1->head = node2->head;
			node1->tail = node2->tail;
		} else {
			node1->tail->next = node2->head;
			node1->tail = node2->tail;
		}
		node1->size += node2->size;
		node2->head = nullptr;
		node2->tail = nullptr;
		node2->size = 0;
	}
	void enqueue(Node *node, Query<T> *txn) {
		if (node->head == nullptr) {
			// empty
			node->head = txn;
			node->tail = txn;
		} else {
			txn->next = nullptr;
			node->tail->next = txn;
			node->tail = txn;
		}
		node->size++;
	}
	bool try_dequeue(Node *node, Query<T> *&txn) {
		auto head_node = node->head;
		if(head_node == nullptr) {
			return false;
		} else if(head_node->next == nullptr) {
			txn = head_node;
			node->head = nullptr;
			node->tail = nullptr;
			return true;
		} else {
			txn = head_node;
			node->head = head_node->next;
			return true;
		}
	}
	Node *find_root(Node *node) {
		auto root_node = node;
		while (root_node != nullptr) {
			if (root_node->parent == nullptr) {
				return root_node;
			} else if (root_node->parent == CLOSED) {
				return nullptr;
			} else {
				root_node = root_node->parent;
			}
		}
		return nullptr;
	}
};

#endif // DBX1000_SCHEDULER_TREE_H
