#ifndef DBX1000_SCHEDULER_TREE_V1_H
#define DBX1000_SCHEDULER_TREE_V1_H


#include "scheduler.h"
#include "database.h"
#include "loader.h"
#include "query.h"
#include "queues.h"
#include <unistd.h>
#include <atomic>
#include <unordered_set>

#define CLOSED (Query<T>)-1
template<typename T>
class SchedulerTreeV1 : public ITransactionQueue<T> {
	typedef Query<T> Node;
	int32_t num_threads;
	SharedQueryQueue<T> input_queue;
	Node** data_nodes;
	Node** active_nodes;
public:
	SchedulerTreeV1(int32_t num_threads) : num_threads(num_threads), input_queue() {
		// create an array to store individual data leaf nodes
		uint64_t size = AccessIterator<T>::get_max_key();
		data_nodes = new Node*[size];
		for (uint64_t i = 0; i < size; i++) {
			data_nodes[i] = nullptr;
		}

		active_nodes = new Node*[num_threads];
		for(int32_t i = 0; i < num_threads; i++) {
			active_nodes[i] = nullptr;
		}
	}

	bool next(int32_t thread_id, Query<T> *&txn) override {
//		if (move_to_next_cc) {
//			if (scheduler_tree->try_pop(chosen_cc)) {
//				pthread_mutex_lock(&chosen_cc->mutex);
//				chosen_cc->done_with_this = false;
//				chosen_cc->owner = thread_id + 1;
//				move_to_next_cc = false;
//			} else {
//				if(abort_buffer.empty()) {
//					this->done = true;
//				}
//				continue;
//			}
//		} else {
//			pthread_mutex_lock(&chosen_cc->mutex);
//		}
//
//		assert(chosen_cc != nullptr);
//		if (chosen_cc->parent == nullptr) {
//			// still a separate connected component
//			if (!chosen_cc->done_with_this) {
//				chosen_query = chosen_cc;
//				chosen_cc->done_with_this = true;
//			} else {
//				if (chosen_cc->txn_queue == nullptr) {
//					// no additional txns in CC
//					move_to_next_cc = true;
//				} else {
//					// get a txn from queue
//					if (chosen_cc->txn_queue->empty()) {
//						move_to_next_cc = true;
//					} else {
//						chosen_query = chosen_cc->txn_queue->front();
//						chosen_cc->txn_queue->pop();
//					}
//				}
//			}
//		} else {
//			// has been merged into a larger CC
//			auto root_cc = find_root<T>(chosen_cc);
//			pthread_mutex_lock(&root_cc->mutex);
//			// check if we can actually delegate!
//			if (root_cc->owner != -1) {
//				if (root_cc->txn_queue == nullptr) {
//					root_cc->txn_queue = new queue<Query<T> *>();
//				}
//				// add own txn
//				if (!chosen_cc->done_with_this) {
//					root_cc->txn_queue->push(chosen_cc);
//					chosen_cc->done_with_this = true;
//				}
//				// add all in the txn_queue
//				if (chosen_cc->txn_queue != nullptr) {
//					while (!chosen_cc->txn_queue->empty()) {
//						root_cc->txn_queue->push(chosen_cc->txn_queue->front());
//						chosen_cc->txn_queue->pop();
//					}
//				}
//				chosen_cc->owner = -1;
//				move_to_next_cc = true;
//			}
//
//			if (root_cc->owner == 0) {
//				// no one has taken responsibility -
//				// so I will!
//				root_cc->owner = thread_id;
//			}
//			pthread_mutex_unlock(&root_cc->mutex);
//		}
//		pthread_mutex_unlock(&chosen_cc->mutex);
//
//		if (move_to_next_cc) {
//			assert(chosen_cc->done_with_this);
//			assert(chosen_cc->txn_queue == nullptr || chosen_cc->txn_queue->empty());
//			chosen_cc = nullptr;
//			continue;
//		}
		return false;
	}

	void add(Query<T>* txn, int32_t thread_id = -1) {
		ReadWriteSet rwset;
		txn->obtain_rw_set(&rwset);
		internal_add(txn, rwset);
	}

protected:
	bool try_enqueue(Node* node, Query<T>* txn) {

		while(true) {
			auto cnode = node->head;
			if(cnode == CLOSED) {
				// empty and closed
				return false;
			} else if(cnode == nullptr) {
				// empty
				if(ATOM_CAS(node->head, nullptr, txn)) {
					return true;
				}
				// oops, someone else inserted - try again
			} else {
				// go to the end - can be either nullptr or closed
				while(!(cnode->next == nullptr || cnode->next == CLOSED)) {
					cnode = cnode->next;
				}

				if(cnode->next == nullptr) {
					txn->next = nullptr;
					if(ATOM_CAS(cnode->next, nullptr, txn)) {
						// successfully planted txn
						return true;
					}
				}
				// oops someone else inserted - try again
			}
		}
	}

	bool try_dequeue(Node* node, Query<T>* & txn) {
		while(true) {
			auto head_node = node->head;
			if(head_node == nullptr || head_node == CLOSED) {
				return false;
			} else {
				if(head_node->next == nullptr) {
					// first mark for delete by changing next to CLOSED
					if(ATOM_CAS(head_node->next, nullptr, CLOSED)) {
						// actually delete by replacing head node with nullptr
						if(ATOM_CAS(node->head, head_node, nullptr)) {
							return true;
						}
					}
				}
			}
		}
	}

	Node* find_root(Node* start_node) {
		auto parent_node = start_node;
		auto root_node = static_cast<Node*>(nullptr);
		while(parent_node != nullptr) {
			root_node = parent_node;
			parent_node = parent_node->next;
		}
		if(root_node != nullptr) {
			if(root_node) {
				return nullptr;
			}
		}
		return root_node;
	}

	void internal_add(Query<T> *txn, ReadWriteSet &rwset) {
		uint64_t num_active_cc = 0;
		uint64_t min_key = UINT64_MAX;
		uint64_t min_key_index = UINT64_MAX;
		for (uint64_t i = 0; i < rwset.num_accesses; i++) {
			auto key = rwset.accesses[i].key;
			auto current_key_cc = data_nodes[key];
			txn->children_roots[i] = find_root(current_key_cc);
			if (root_cc[i] != nullptr) {
				num_active_cc++;
				if (key < min_key) {
					min_key_index = i;
					min_key = key;
				}
			}
		}

		auto selected_cc = static_cast<Query<T> *>(nullptr);
		if (num_active_cc > 0) {
			selected_cc = root_cc[min_key_index];
			pthread_mutex_lock(&selected_cc->mutex);
			if (selected_cc->owner == -1) {
				// oops! worker marked it done! we need to do this again :(
				pthread_mutex_unlock(&selected_cc->mutex);
				return schedule(new_query, rwset);
			} else {
				if (selected_cc->txn_queue == nullptr) {
					selected_cc->txn_queue = new queue<Query<T> *>();
				}
				selected_cc->txn_queue->push(new_query);
				num_delegated++;
				assert(selected_cc->owner != -1);
				pthread_mutex_unlock(&selected_cc->mutex);
			}
		} else {
			selected_cc = new_query;
			selected_cc->parent = nullptr;
			selected_cc->owner = 0;
			scheduler_tree.push(selected_cc);
			round_robin_count++;
			num_submitted++;
		}

		// let our data structures know about our scheduling decision
		for (uint64_t i = 0; i < rwset.num_accesses; i++) {
			auto key = rwset.accesses[i].key;
			auto current_key_cc = (Query<T> *)data_next_pointer[key];
			if (current_key_cc != selected_cc) {
				// update data_next_pointer array so that all future txns
				// that touch this gets sent to this new CC
				while (!ATOM_CAS(data_next_pointer[key], (int64_t)current_key_cc,
				                 (int64_t)selected_cc)) {
					current_key_cc = (Query<T> *)data_next_pointer[key];
				}

				// update all other CC that txn touches such that
				// root of their CC points to selected CC
				if (current_key_cc != nullptr) {
					pthread_mutex_lock(&current_key_cc->mutex);
					auto current_key_root = find_root<T>(current_key_cc);
					if (current_key_root != selected_cc) {
						current_key_cc->parent = selected_cc;
					}
					pthread_mutex_unlock(&current_key_cc->mutex);
				}
			}
		}
	}

};


#endif //DBX1000_SCHEDULER_TREE_V1_H
