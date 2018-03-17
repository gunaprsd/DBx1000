#ifndef DBX1000_SCHEDULER_TREE_H
#define DBX1000_SCHEDULER_TREE_H

#include "database.h"
#include "loader.h"
#include "query.h"
#include "queues.h"
#include "scheduler.h"
#include <atomic>
#include <unistd.h>
#include <unordered_set>
using namespace std;

#define CLOSED (Query<T> *)-1

template <typename T> class SchedulerTree : public ITransactionQueue<T> {
    typedef Query<T> Node;
    int32_t num_threads;
    SharedQueryQueue<T> input_queue;
    Node **data_nodes;
    Node **active_nodes;

  public:
    SchedulerTree(int32_t num_threads) : num_threads(num_threads), input_queue() {
        // create an array to store individual data leaf nodes
        uint64_t size = AccessIterator<T>::get_max_key();
        data_nodes = new Node *[size];
        for (uint64_t i = 0; i < size; i++) {
            data_nodes[i] = nullptr;
        }

        active_nodes = new Node *[num_threads];
        for (int32_t i = 0; i < num_threads; i++) {
            active_nodes[i] = nullptr;
        }
    }
    bool next(int32_t thread_id, Query<T> *&txn) override {
        // get a new position node in the tree, if currently null
	    while(true) {
		    auto active_node = active_nodes[thread_id];
		    if (active_node == nullptr) {
			    if (!input_queue.try_pop(active_node)) {
				    return false;
			    }
		    }

		    if (try_dequeue(active_node, txn)) {
			    // try to dequeue something - if successful return
			    return true;
		    } else if (active_node->parent == nullptr) {
			    // try to close the queue so that no more txns are added
			    if (try_close_queue(active_node)) {
				    // if queue is closed, close the parent
				    if (try_close_parent(active_node)) {
					    // get a new node in next iteration
					    active_nodes[thread_id] = nullptr;
				    }
				    // if CAS fails, retry and process appropriately
			    }
			    // if cannot close queue, retry and process appropriately
		    } else if(active_node->parent == CLOSED) {
			    // get a new node in next iteration
				active_nodes[thread_id] = nullptr;
		    } else {
			    auto parent = active_node->parent;
			    auto val = ATOM_SUB_FETCH(parent->num_active_children, 1);
			    if (val == 0) {
				    // process parent in next iteration
				    active_nodes[thread_id] = parent;
			    } else {
				    // get a new node in next iteration
				    active_nodes[thread_id] = nullptr;
			    }
		    }
	    }
    }
    void add(Query<T> *txn, int32_t thread_id = -1) {
        ReadWriteSet rwset;
        txn->obtain_rw_set(&rwset);
        internal_add(txn, rwset);
    }
  protected:
	void internal_add(Query<T> *txn, ReadWriteSet &rwset) {
		unordered_set<Node *> root_nodes;
		int64_t num_active_children = 0;
		for (uint32_t i = 0; i < rwset.num_accesses; i++) {
			auto key = rwset.accesses[i].key;
			auto data_node = data_nodes[key];
			auto root_node = find_root(data_node);
			if (root_node != nullptr) {
				if (root_nodes.find(root_node) == root_nodes.end()) {
					root_nodes.insert(root_node);
					num_active_children++;
				}
			}
		}

		if (root_nodes.size() == 1) {
			auto root_node = *(root_nodes.begin());
			if (try_enqueue(root_node, txn)) {
				// we are done!
			} else {
				return internal_add(txn, rwset);
			}
		} else {
			auto root_node = txn;
			root_node->parent = nullptr;
			root_node->next = nullptr;
			root_node->head = nullptr;
			try_enqueue(root_node, txn);
			root_node->num_active_children = num_active_children;

			auto val = 1;
			for (auto child_node : root_nodes) {
				if (!ATOM_CAS(child_node->parent, root_node, nullptr)) {
					// must have been closed by the worker!
					val = ATOM_SUB_FETCH(root_node->num_active_children, 1);
				}
			}

			for (uint32_t i = 0; i < rwset.num_accesses; i++) {
				auto key = rwset.accesses[i].key;
				data_nodes[key] = root_node;
			}

			if (val == 0) {
				input_queue.push(root_node);
			}
		}
	}
	bool try_enqueue(Node *node, Query<T> *txn) {
        while (true) {
            auto cnode = node->head;
            if (cnode == CLOSED) {
                // empty and closed
                return false;
            } else if (cnode == nullptr) {
                // empty
                if (ATOM_CAS(node->head, nullptr, txn)) {
                    return true;
                }
                // oops, someone else inserted - try again
            } else {
                // go to the end - can be either nullptr or closed
                while (!(cnode->next == nullptr || cnode->next == CLOSED)) {
                    cnode = cnode->next;
                }

                if (cnode->next == nullptr) {
                    txn->next = nullptr;
                    if (ATOM_CAS(cnode->next, nullptr, txn)) {
                        // successfully planted txn
                        return true;
                    }
                }
                // oops someone else inserted - try again
            }
        }
    }
    bool try_dequeue(Node *node, Query<T> *&txn) {
        while (true) {
            auto head_node = node->head;
            if (head_node == nullptr || head_node == CLOSED) {
                return false;
            } else {
                if (head_node->next == nullptr) {
                    // first mark for delete by changing next to CLOSED
                    if (ATOM_CAS(head_node->next, nullptr, CLOSED)) {
                        // actually delete by replacing head node with nullptr
                        if (ATOM_CAS(node->head, head_node, nullptr)) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    Node *find_root(Node *start_node) {
        auto parent_node = start_node;
        auto root_node = static_cast<Node *>(nullptr);
        while (parent_node != nullptr && parent_node != CLOSED) {
            root_node = parent_node;
            parent_node = parent_node->next;
        }
        if (root_node != nullptr) {
            if (is_parent_closed(root_node)) {
                return nullptr;
            }
        }
        return root_node;
    }
    bool is_queue_closed(Node *node) { return node->head == CLOSED; }
    bool is_parent_closed(Node *node) { return node->parent == CLOSED; }
	bool try_close_queue(Node *node) {
		if (node->head == CLOSED) {
			return true;
		} else if (node->head == nullptr) {
			if (ATOM_CAS(node->head, nullptr, CLOSED)) {
				return true;
			}
		}
		return false;
	}
    bool try_close_parent(Node *node) { return ATOM_CAS(node->parent, nullptr, CLOSED); }

};

#endif // DBX1000_SCHEDULER_TREE_H
