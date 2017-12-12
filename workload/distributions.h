// Copyright [2017] <Guna Prasaad>

#include "global.h"

#ifndef WORKLOAD_DISTRIBUTIONS_H_
#define WORKLOAD_DISTRIBUTIONS_H_

/*
 * RandomNumberGenerator:
 * ----------------------
 * Generates 64bit and double random numbers using lrand48_r and drand48_r
 * Each thread has its own buffer data, so individually the threads generate
 * random distribution.
 */
class RandomNumberGenerator {
public:
  explicit RandomNumberGenerator(uint32_t num_threads) {
    _num_threads = num_threads;
    _buffers = new drand48_data *[num_threads];
    for(uint32_t i = 0; i < num_threads; i++) {
      _buffers[i] = reinterpret_cast<drand48_data *>(_mm_malloc(sizeof(drand48_data), 64));
    }
  }
  ~RandomNumberGenerator() { delete[] _buffers; }
  uint64_t nextInt64(uint32_t thread_id) {
    int64_t rint64;
    lrand48_r(_buffers[thread_id], &rint64);
    return static_cast<uint64_t>(rint64);
  }
  double nextDouble(uint32_t thread_id) {
    double rdouble;
    drand48_r(_buffers[thread_id], &rdouble);
    return rdouble;
  }
  void seed(uint32_t thread_id, long value) {
    srand48_r(value, _buffers[thread_id]);
  }
protected:
  uint32_t _num_threads;
  drand48_data **_buffers;
};

/*
 * ZipfianNumberGenerator:
 * ----------------
 * is developed based on the paper 'Quickly Generating
 * Billion-Record Synthetic Databases' from SIGMOD 1994. This is a concurrent
 * version of the zipfian generator, where each thread uses a different random
 * buffer and hence the sequence generated for each thread is individually
 * zipfian.
 *
 * It generates integers between [1, n) where integer k gets weight
 * proportional to (1/k)^theta; theta is the skew and is typically
 * between 0 and 1 (higher is more skewed).
 *
 * Potential typo in the paper (fixed): using zeta(2, theta) instead
 * of zeta(theta, 2).
 */
class ZipfianNumberGenerator {
public:
  ZipfianNumberGenerator(uint64_t n, uint32_t num_threads, double zipfian_theta)
      : _generator(num_threads) {
    _the_n = n - 1;
    _theta = zipfian_theta;
    initialize();
  }
  uint64_t nextInt64(uint32_t thread_id) {
    double u = _generator.nextDouble(thread_id);
    double uz = u * _zeta_n_theta;
    if (uz < 1) {
      return 1;
    } else if (uz < 1 + pow(0.5, _theta)) {
      return 2;
    } else {
      return 1 + (uint64_t)(_the_n * pow(_eta * u - _eta + 1, _alpha));
    }
  }
  void seed(uint32_t thread_id, long value){
    _generator.seed(thread_id, value);
  }
  ~ZipfianNumberGenerator() = default;

protected:
  void initialize() {
    _zeta_2_theta = zeta(2, _theta);
    _zeta_n_theta = zeta(_the_n, _theta);
    _alpha = 1 / (1 - _theta);
    _eta = (1 - pow(2.0 / _the_n, 1 - _theta)) /
           (1 - _zeta_2_theta / _zeta_n_theta);
  }

  static double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++) {
      sum += pow(1.0 / i, theta);
    }
    return sum;
  }

  double _theta;
  uint64_t _the_n;
  double _zeta_n_theta;
  double _zeta_2_theta;
  double _alpha;
  double _eta;
  RandomNumberGenerator _generator;
};

#endif // WORKLOAD_DISTRIBUTIONS_H_
