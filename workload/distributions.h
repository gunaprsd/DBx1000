// Copyright [2017] <Guna Prasaad>

#include "global.h"
#include <pthread.h>

#ifndef WORKLOAD_DISTRIBUTIONS_H_
#define WORKLOAD_DISTRIBUTIONS_H_

/*
 * RandomNumberGenerator:
 * ----------------------
 * Generates 64bit and double random numbers using lrand48_r and drand48_r
 * Each thread has its own buffer data, so individually the threads generate
 * random distribution.
 *
 * Note that this is also thread-safe. It basically produces n sequences of
 * numbers in which each sequence follows a random distribution.
 */
class RandomNumberGenerator {
public:
  explicit RandomNumberGenerator(uint64_t num_dists) {
    _num_dists = num_dists;
    _buffers = new drand48_data *[num_dists];
    _latches = new pthread_mutex_t[num_dists];
    for (uint64_t i = 0; i < num_dists; i++) {
      _buffers[i] = reinterpret_cast<drand48_data *>(
          _mm_malloc(sizeof(drand48_data), 64));
      pthread_mutex_init(&_latches[i], NULL);
    }
  }
  ~RandomNumberGenerator() {
    for (uint64_t i = 0; i < _num_dists; i++) {
      pthread_mutex_destroy(&_latches[i]);
      free(_buffers[i]);
    }
    delete[] _buffers;
    delete[] _latches;
  }
  uint64_t nextInt64(uint64_t dist_id) {
    int64_t rint64;
    pthread_mutex_lock(&_latches[dist_id]);
    lrand48_r(_buffers[dist_id], &rint64);
    pthread_mutex_unlock(&_latches[dist_id]);
    return static_cast<uint64_t>(rint64);
  }
  double nextDouble(uint64_t dist_id) {
    double rdouble;
    pthread_mutex_lock(&_latches[dist_id]);
    drand48_r(_buffers[dist_id], &rdouble);
    pthread_mutex_unlock(&_latches[dist_id]);
    return rdouble;
  }
  void seed(uint64_t dist_id, uint64_t value) {
    pthread_mutex_lock(&_latches[dist_id]);
    srand48_r((long)value, _buffers[dist_id]);
    pthread_mutex_unlock(&_latches[dist_id]);
  }

protected:
  uint64_t _num_dists;
  drand48_data **_buffers;
  pthread_mutex_t *_latches;
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
  ZipfianNumberGenerator(uint64_t n, uint64_t num_distributions,
                         double zipfian_theta)
      : _generator(num_distributions) {
    _the_n = n - 1;
    _theta = zipfian_theta;
    initialize();
  }
  uint64_t nextZipfInt64(uint64_t dist_id) {
    double u = _generator.nextDouble(dist_id);
    double uz = u * _zeta_n_theta;
    if (uz < 1) {
      return 1;
    } else if (uz < 1 + pow(0.5, _theta)) {
      return 2;
    } else {
      return 1 + (uint64_t)(_the_n * pow(_eta * u - _eta + 1, _alpha));
    }
  }
  uint64_t nextRandInt64(uint64_t dist_id) {
    return _generator.nextInt64(dist_id);
  }
  double nextRandDouble(uint64_t dist_id) {
    return _generator.nextDouble(dist_id);
  }
  void seed(uint64_t dist_id, uint64_t value) { _generator.seed(dist_id, value); }
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
