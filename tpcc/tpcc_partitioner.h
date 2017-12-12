// Copyright [2017] <Guna Prasaad>

#ifndef TPCC_TPCC_PARTITIONER_H_
#define TPCC_TPCC_PARTITIONER_H_

#include "conflict_graph_partitioner.h"
#include "access_graph_partitioner.h"
#include "tpcc.h"

class TPCCConflictGraphPartitioner : public ConflictGraphPartitioner {
public:
		void initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size,
										uint32_t parallelism, const char *dest_folder_path) override;

		BaseQueryList *get_queries_list(uint32_t thread_id) override;
		void partition() override;

protected:
		void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;
		tpcc_query **_partitioned_queries;

private:
		int compute_weight(BaseQuery *bq1, BaseQuery *bq2) override {
			auto q1 = (tpcc_query *)bq1;
			auto q2 = (tpcc_query *)bq2;
			if (q1->type == TPCC_PAYMENT_QUERY && q2->type == TPCC_PAYMENT_QUERY) {
				return compute_weight((tpcc_payment_params *)&q1->params,
															(tpcc_payment_params *)&q2->params);
			} else if (q1->type == TPCC_NEW_ORDER_QUERY &&
								 q2->type == TPCC_NEW_ORDER_QUERY) {
				return compute_weight((tpcc_new_order_params *)&q1->params,
															(tpcc_new_order_params *)&q2->params);
			} else if (q1->type == TPCC_PAYMENT_QUERY &&
								 q2->type == TPCC_NEW_ORDER_QUERY) {
				return compute_weight((tpcc_payment_params *)&q1->params,
															(tpcc_new_order_params *)&q2->params);
			} else {
				return compute_weight((tpcc_payment_params *)&q2->params,
															(tpcc_new_order_params *)&q1->params);
			}
		}

		int compute_weight(tpcc_payment_params *q1, tpcc_payment_params *q2) {
			// Don't know if c_last and c_id match - so ignore!
			if (q1->w_id == q2->w_id) {
				if (q1->d_id == q2->d_id) {
					return 10;
				} else {
					return 9;
				}
			} else {
				return -1;
			}
		}

		int compute_weight(tpcc_new_order_params *q1, tpcc_new_order_params *q2) {
			int weight = -1;
			if (q1->w_id == q2->w_id) {
				if (q1->d_id == q2->d_id) {
					weight += 20;
				} else {
					weight += 19;
				}
			}

			for (uint32_t i = 0; i < q1->ol_cnt; i++) {
				for (uint32_t j = 0; j < q2->ol_cnt; j++) {
					if (q1->items[i].ol_supply_w_id == q2->items[i].ol_supply_w_id &&
							q1->items[i].ol_i_id == q2->items[i].ol_i_id) {
						weight += 1;
					}
				}
			}

			return weight;
		}

		int compute_weight(tpcc_payment_params *q1, tpcc_new_order_params *q2) {
			if (q1->w_id == q2->w_id) {
				if (q1->d_id == q2->d_id) {
					if (q1->c_id == q2->c_id) {
						return 10;
					} else {
						return 9;
					}
				} else {
					return 8;
				}
			} else {
				return -1;
			}
		}
};

class TPCCAccessGraphPartitioner : public AccessGraphPartitioner {
public:
		void initialize(BaseQueryMatrix *queries, uint64_t max_cluster_graph_size,
										uint32_t parallelism, const char *dest_folder_path) override;
		void partition() override;

protected:
		void first_pass();
		void second_pass();
		void third_pass();
		void compute_post_stats(idx_t *parts);
		inline uint32_t get_hash(uint64_t key) { return static_cast<uint32_t>(key); }
		void partition_per_iteration() override;
		void per_thread_write_to_file(uint32_t thread_id, FILE *file) override;

		tpcc_query **_partitioned_queries;
		TxnDataInfo *_info_array;

		vector<idx_t> vwgt;
		vector<idx_t> adjwgt;
		vector<idx_t> xadj;
		vector<idx_t> adjncy;
};

#endif //TPCC_TPCC_PARTITIONER_H_
