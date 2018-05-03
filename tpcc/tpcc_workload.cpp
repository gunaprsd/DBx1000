#include "tpcc_workload.h"
#include "scheduler.h"
#include "online_batch_scheduler.h"
#include "online_batch_scheduler_v4.h"

TPCCWorkloadGenerator::TPCCWorkloadGenerator(const TPCCBenchmarkConfig &_config,
                                             uint64_t num_threads, uint64_t size_per_thread,
                                             const string &folder_path)
    : ParallelWorkloadGenerator<tpcc_params>(num_threads, size_per_thread, folder_path),
      config(_config), helper(_config.num_warehouses), _random(num_threads) {
    for (uint64_t i = 0; i < _config.num_warehouses; i++) {
        helper.random.seed(i, i + 1);
    }

    for (uint64_t i = 0; i < num_threads; i++) {
        _random.seed(i, 2 * i + 1);
    }

    num_orders = new uint64_t[num_threads];
    num_order_txns = new uint64_t[num_threads];
    for (uint64_t i = 0; i < num_threads; i++) {
        num_orders[i] = 0;
        num_order_txns[i] = 0;
    }
}

void TPCCWorkloadGenerator::per_thread_generate(uint64_t thread_id) {
    for (uint64_t i = 0; i < _num_queries_per_thread; i++) {
        double x = _random.nextDouble(thread_id);
        if (x < config.percent_payment) {
            _queries[thread_id][i].type = TPCC_PAYMENT_QUERY;
            gen_payment_request(thread_id, &_queries[thread_id][i]);
        } else {
            _queries[thread_id][i].type = TPCC_NEW_ORDER_QUERY;
            gen_new_order_request(thread_id, &_queries[thread_id][i]);
        }
    }
    double avg = ((double)num_orders[thread_id]) / ((double)num_order_txns[thread_id]);
    printf("Thread : %lu, Num-Order-Txns: %lu, Avg-Orders-Per-Txn: %lf\n", thread_id,
           num_order_txns[thread_id], avg);
}



TPCCExecutor::TPCCExecutor(const TPCCBenchmarkConfig &config, const string &input_file,
                           uint64_t num_threads)
    : _db(config),
      _loader(input_file) {

    // Build database in parallel
    _db.initialize(FLAGS_threads);
    _db.load();

    // Load workload in parallel
    _loader.load();

    // Initialize each thread
    if(FLAGS_scheduler_type == "online_batch") {
	    _scheduler = new OnlineBatchScheduler<tpcc_params>(num_threads, FLAGS_scheduler_batch_size, &_db);
    } else if(FLAGS_scheduler_type == "online_batch_v2") {
	    _scheduler = new OnlineBatchSchedulerV2<tpcc_params>(num_threads, FLAGS_scheduler_batch_size, &_db);
    } else if(FLAGS_scheduler_type == "online_batch_v3") {
        _scheduler = new OnlineBatchSchedulerV3<tpcc_params>(num_threads, FLAGS_scheduler_batch_size, &_db);
    } else if(FLAGS_scheduler_type == "online_batch_v4") {
        _scheduler = new OnlineBatchSchedulerV4<tpcc_params>(num_threads, FLAGS_scheduler_batch_size, &_db);
    } else {
    	_scheduler = new OnlineScheduler<tpcc_params>(num_threads, &_db);
    }
}

void TPCCExecutor::execute() {
    _scheduler->schedule(&_loader);
}

void TPCCExecutor::release() {}
