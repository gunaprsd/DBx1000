#ifndef DBX1000_ABORT_BUFFER_H
#define DBX1000_ABORT_BUFFER_H

#include "global.h"
#include "query.h"
#include "distributions.h"
#include <functional>
#include <queue>
#include <tuple>
#define QUEUE_SIZE 100
using namespace std;


typedef tuple<uint64_t, BaseQuery *> ptype;
auto cmp = [](ptype t1, ptype t2) { return get<0>(t1) < get<0>(t2); };

template <typename T> class TimedAbortBuffer {
	priority_queue<ptype, vector<ptype>, decltype(cmp)> priorityQueue;
	RandomNumberGenerator gen;

public:
	TimedAbortBuffer() : priorityQueue(cmp), gen(1) {}

	void add_query(Query<T> *query) {
		uint64_t penalty = 0;
		if (ABORT_PENALTY != 0) {
			double r = gen.nextDouble(0);
			penalty = static_cast<uint64_t>(r * FLAGS_abort_penalty);
		}
		uint64_t ready_time = get_sys_clock() + penalty;
		auto t = make_tuple(ready_time, (BaseQuery *)query);
		priorityQueue.push(t);
	}

	bool get_ready_query(Query<T> *&query) {
		if (priorityQueue.empty()) {
			query = nullptr;
			return false;
		} else {

			// ensure you wait and get a query from buffer
			// if it is getting filled up beyond buffer size
			while (priorityQueue.size() >= (size_t)FLAGS_abort_buffer_size) {
				auto current_time = get_sys_clock();
				auto top_time = get<0>(priorityQueue.top());
				if (current_time < top_time) {
					usleep(top_time - current_time);
				} else {
					query = reinterpret_cast<Query<T> *>(get<1>(priorityQueue.top()));
					priorityQueue.pop();
					return true;
				}
			}

			// remove a query from buffer only if it is ready
			auto current_time = get_sys_clock();
			auto top_time = get<0>(priorityQueue.top());
			if (top_time < current_time) {
				query = reinterpret_cast<Query<T> *>(get<1>(priorityQueue.top()));
				priorityQueue.pop();
				return true;
			} else {
				query = nullptr;
				return false;
			}
		}
	}

	bool empty() { return priorityQueue.empty(); }
};

#endif //DBX1000_ABORT_BUFFER_H
