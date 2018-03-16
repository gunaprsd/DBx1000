#ifndef DBX1000_PARALLEL_QUEUES_H
#define DBX1000_PARALLEL_QUEUES_H

#include "query.h"
#include "distributions.h"

template<typename T>
class ParallelQueues : public ITransactionQueue<T> {
	int32_t num_threads;
	SharedQueryQueue<T>* input_queues;
	RandomNumberGenerator gen;
public:
	ParallelQueues(int32_t num_threads) : num_threads(num_threads), input_queues(nullptr), gen(1) {
		input_queues = new SharedQueryQueue<T>[num_threads];
		gen.seed(0, FLAGS_seed + 251);
	}
	bool next(int32_t thread_id, Query<T> *&txn) override {
		return input_queues[thread_id].try_pop(txn);
	}
	void add(Query<T> *txn, int32_t thread_id = -1) override {
		if(thread_id == -1) {
			thread_id = static_cast<int32_t>(gen.nextInt64(0) % num_threads);
		}
		input_queues[thread_id].push(txn);
	}
};

template<typename T>
class SharedQueue : public ITransactionQueue<T> {
	int32_t num_threads;
	SharedQueryQueue<T> input_queue;
public:
	SharedQueue(int32_t num_threads) : num_threads(num_threads), input_queue() {}
	bool next(int32_t thread_id, Query<T> *&txn) override {
		return input_queue.try_pop(txn);
	}
	void add(Query<T> *txn, int32_t thread_id = -1) override {
		input_queue.push(txn);
	}
};

#endif //DBX1000_PARALLEL_QUEUES_H
