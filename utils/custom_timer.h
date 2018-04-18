#ifndef __CUSTOM_TIMER_H__
#define __CUSTOM_TIMER_H__

//#define USE_CHRONO 
#include "global.h"
#include "helper.h"
#include <chrono>
using namespace std;

class CustomTimer {
    typedef chrono::high_resolution_clock::time_point TimePoint;

  public:
    CustomTimer() = default;
    void Start() { 
        #ifdef USE_CHRONO
        start_time = chrono::high_resolution_clock::now(); 
        #else 
        start_time = get_server_clock();
        #endif
    }
    void Stop() { 
    #ifdef USE_CHRONO
        end_time = chrono::high_resolution_clock::now();
    #else
        end_time = get_server_clock();
    #endif 
    }
    double DurationInSecs() {
        #ifdef USE_CHRONO
        return chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count() / (double)1000.0;
        #else 
        return DURATION(end_time, start_time);
        #endif
    }
    double DurationInNanoSecs() {
        #ifdef USE_CHRONO
        return chrono::duration_cast<chrono::nanoseconds>(end_time - start_time).count();
        #else 
        return (end_time - start_time);
        #endif
    }

  private:
  #ifdef USE_CHRONO
    TimePoint start_time;
    TimePoint end_time;
    #else 
    uint64_t start_time, end_time;
    #endif
};
#endif