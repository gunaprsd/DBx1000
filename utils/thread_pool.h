#ifndef __UTILS_PARALLEL_EXECUTOR_H__
#define __UTILS_PARALLEL_EXECUTOR_H__
#include <pthread.h>
#include <functional>
#include "global.h"
using namespace std;
class ParallelExecutor {
  public:
    ParallelExecutor(long num_threads, ThreadLocalData* data, void *(*func)(void *)) {}
    void Run();
    long GetDurationInSecs();
    uint64_t GetDuration();
  protected:
    uint64_t start_time;
    uint64_t end_time;
    long num_threads;
    pthread_t* threads;
    ThreadLocalData* data;
    void* (*func)(void*);
};

#endif