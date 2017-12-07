//
// Created by Guna Prasaad on 07/12/17.
//

#ifndef DBX1000_PARALLEL_WORKLOAD_PARTITIONER_H
#define DBX1000_PARALLEL_WORKLOAD_PARTITIONER_H

#include "graph_partitioner.h"

/*
 * ParallelWorkloadPartitioner:
 * ----------------------------
 * Partitions a given workload using the METIS graph partitioner
 * Any benchmark has to implement a compute weight function that is invoked
 * for every pair of transactions.
 */
class ParallelWorkloadPartitioner
{
public:
		virtual void   		initialize                 (BaseQueryMatrix * queries,
																									uint64_t max_cluster_graph_size,
																									uint32_t parallelism,
																									const char * dest_folder_path);
		virtual void    	partition                  ();
		void 			write_to_files						 ();
		void    	print_execution_summary		 ();
		void 		 	print_partition_summary		 ();
protected:
		virtual void    	compute_data_info          ();
		void    	partition_per_iteration		 ();
		void    	write_pre_partition_file   ();
		void    	write_post_partition_file  ();
		void 			parallel_compute_post_stats(METISGraphPartitioner * partitioner);
		ParCSRGraph * parallel_create_graph			 ();
		CSRGraph *   	create_graph							 ();
		static 	void * 		create_graph_helper				 (void * data);
		static 	void * 		compute_statistics_helper	 (void * data);

		//Iteration sensitive -> depends on value of _current_iteration
		void 			get_query						(uint64_t qid, BaseQuery * & query);
		uint32_t	get_array_idx				(uint64_t qid);

		//Standard options
		uint32_t    _num_arrays;
		uint64_t    _num_queries_per_array;
		uint64_t		_max_cluster_graph_size;
		uint64_t    _num_queries_per_iter_per_array;

		//Degree of parallelism to use for each iteration
		uint32_t 		_parallelism;

		//Fields valid for each iteration
		uint32_t		_current_iteration;
		uint64_t		_array_iter_start_offset;
		uint64_t		_total_num_edges;
		uint64_t		_total_pre_cross_core_edges;
		uint64_t		_total_pre_cross_core_weight;
		uint64_t		_total_post_cross_core_edges;
		uint64_t		_total_post_cross_core_weight;

		std::vector<BaseQuery*>*    _tmp_queries;
		uint32_t *                  _tmp_array_sizes;
		BaseQueryMatrix *           _orig_queries;
		char 												_folder_path[200];

		double data_statistics_duration;
		double graph_init_duration;
		double partition_duration;
		double shuffle_duration;

/** Need to be implemented by benchmarks */
public:
		virtual BaseQueryList * get_queries_list(uint32_t thread_id) = 0;
protected:
		virtual int     compute_weight				(BaseQuery * q1, BaseQuery * q2) = 0;
		virtual void 		per_thread_write_to_file(uint32_t thread_id, FILE *file) = 0;
};


inline void ParallelWorkloadPartitioner::get_query(uint64_t qid, BaseQuery * & query) {
	auto array_idx 		= static_cast<uint32_t>(qid % _num_arrays);
	auto array_offset = static_cast<uint32_t>((qid / _num_arrays) + _array_iter_start_offset);
	_orig_queries->get(array_idx, array_offset, query);
}

inline uint32_t ParallelWorkloadPartitioner::get_array_idx(uint64_t qid) {
	return static_cast<uint32_t>(qid % _num_arrays);
}

#endif //DBX1000_PARALLEL_WORKLOAD_PARTITIONER_H
