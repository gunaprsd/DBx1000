#include "thread_pool.h"

 ParallelExecutor::ParallelExecutor(long _num_threads, ThreadLocalData* _data, void *(*_func)(void *)) {
     num_threads = _num_threads;
     data = _data;
     func = _func;
     threads = new pthread_t[num_threads];
     start_time = 0;
     end_time = 0;
}

void ParallelExecutor::Run() {
    start_time = get_server_clock();
    for (uint64_t i = 0; i < _num_threads; i++) {
        pthread_create(&threads[i], nullptr, func, (void *)&data[i]);
    }
    for (uint32_t i = 0; i < _num_threads; i++) {
        pthread_join(threads[i], nullptr);
    }
    end_time = get_server_clock();
}

uint64_t ParallelExecutor::GetDuration() {
    return (end_time - start_time);
}

double ParallelExecutor::GetDurationInSecs() {
    return DURATION(end_time, start_time);
}

