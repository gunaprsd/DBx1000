#include "Statistics.h"
#include "Allocator.h"
#include "Global.h"
#include "Helper.h"

#define BILLION 1000000000UL

void ThreadStatistics::initialize(ThreadId thd_id) {
	clear();
	all_debug1 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, 64);
	all_debug2 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, 64);
}

void ThreadStatistics::clear() {
	txn_cnt = 0;
	abort_cnt = 0;
	run_time = 0;
	time_man = 0;
	debug1 = 0;
	debug2 = 0;
	debug3 = 0;
	debug4 = 0;
	debug5 = 0;
	time_index = 0;
	time_abort = 0;
	time_cleanup = 0;
	time_wait = 0;
	time_ts_alloc = 0;
	latency = 0;
	time_query = 0;
}

void ThreadMonitor::init() {
	clear();
}

void ThreadMonitor::clear() {	
	time_man = 0;
	time_index = 0;
	time_wait = 0;
}

void Statistics::initialize() {
	if (!STATS_ENABLE) 
		return;
	_stats = (ThreadStatistics**) 
			_mm_malloc(sizeof(ThreadStatistics*) * g_thread_cnt, 64);
	_monitors = (ThreadMonitor**) 
			_mm_malloc(sizeof(ThreadMonitor*) * g_thread_cnt, 64);
	dl_detect_time = 0;
	dl_wait_time = 0;
	deadlock = 0;
	cycle_detect = 0;
}

void Statistics::initialize(ThreadId thread_id) {
	if (!STATS_ENABLE) 
		return;
	_stats[thread_id] = (ThreadStatistics *) 
		_mm_malloc(sizeof(ThreadStatistics), 64);
	_monitors[thread_id] = (ThreadMonitor *)
		_mm_malloc(sizeof(ThreadMonitor), 64);

	_stats[thread_id]->initialize(thread_id);
	_monitors[thread_id]->init();
}

void Statistics::clear(ThreadId tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		_monitors[tid]->clear();

		dl_detect_time = 0;
		dl_wait_time = 0;
		cycle_detect = 0;
		deadlock = 0;
	}
}

void Statistics::add_debug(ThreadId thd_id, uint64_t value, uint32_t select) {
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		if (select == 1)
			_stats[thd_id]->all_debug1[tnum] = value;
		else if (select == 2)
			_stats[thd_id]->all_debug2[tnum] = value;
	}
}

void Statistics::commit(ThreadId thd_id) {
	if (STATS_ENABLE) {
		_stats[thd_id]->time_man += _monitors[thd_id]->time_man;
		_stats[thd_id]->time_index += _monitors[thd_id]->time_index;
		_stats[thd_id]->time_wait += _monitors[thd_id]->time_wait;
		_monitors[thd_id]->init();
	}
}

void Statistics::abort(ThreadId thd_id) {
	if (STATS_ENABLE) 
		_monitors[thd_id]->init();
}

void Statistics::print() {
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	double total_run_time = 0;
	double total_time_man = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_time_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_wait = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_time_query = 0;
	double total_throughput = 0;
	for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		total_run_time += _stats[tid]->run_time;
		total_throughput += (_stats[tid]->txn_cnt * BILLION / _stats[tid]->run_time);
		total_time_man += _stats[tid]->time_man;
		total_debug1 += _stats[tid]->debug1;
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_time_index += _stats[tid]->time_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_wait += _stats[tid]->time_wait;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_time_query += _stats[tid]->time_query;
		
		printf("[tid=%ld] txn_cnt=%ld,abort_cnt=%ld\n", 
			tid,
			_stats[tid]->txn_cnt,
			_stats[tid]->abort_cnt
		);
	}
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "w");
		fprintf(outf, "[summary] txn_cnt=%ld, abort_cnt=%ld"
			", run_time=%f, throughput=%f, time_wait=%f, time_ts_alloc=%f"
			", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
			", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
			", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f\n",
			total_txn_cnt, 
			total_abort_cnt,
			total_run_time / BILLION,
			total_throughput,
			total_time_wait / BILLION,
			total_time_ts_alloc / BILLION,
			(total_time_man - total_time_wait) / BILLION,
			total_time_index / BILLION,
			total_time_abort / BILLION,
			total_time_cleanup / BILLION,
			total_latency / BILLION / total_txn_cnt,
			deadlock,
			cycle_detect,
			dl_detect_time / BILLION,
			dl_wait_time / BILLION,
			total_time_query / BILLION,
			total_debug1, // / BILLION,
			total_debug2, // / BILLION,
			total_debug3, // / BILLION,
			total_debug4, // / BILLION,
			total_debug5 / BILLION
		);
		fclose(outf);
	}
	printf("[summary] txn_cnt=%ld, abort_cnt=%ld"
		", run_time=%f, throughput=%f, time_wait=%f, time_ts_alloc=%f"
		", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
		", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
		", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f\n", 
		total_txn_cnt, 
		total_abort_cnt,
		total_run_time / BILLION,
		total_throughput,
		total_time_wait / BILLION,
		total_time_ts_alloc / BILLION,
		(total_time_man - total_time_wait) / BILLION,
		total_time_index / BILLION,
		total_time_abort / BILLION,
		total_time_cleanup / BILLION,
		total_latency / BILLION / total_txn_cnt,
		deadlock,
		cycle_detect,
		dl_detect_time / BILLION,
		dl_wait_time / BILLION,
		total_time_query / BILLION,
		total_debug1 / BILLION,
		total_debug2, // / BILLION,
		total_debug3, // / BILLION,
		total_debug4, // / BILLION,
		total_debug5  // / BILLION 
	);
	if (g_prt_lat_distr)
		print_lat_distr();
}

void Statistics::print_lat_distr() {
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "a");
		for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) {
			fprintf(outf, "[all_debug1 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug1[tnum]);
			fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug2[tnum]);
			fprintf(outf, "\n");
		}
		fclose(outf);
	} 
}
