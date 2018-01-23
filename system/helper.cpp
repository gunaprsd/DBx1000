#include "helper.h"
#include "global.h"
#include "mem_alloc.h"
#include "time.h"
#include <cstring>

bool itemid_t::operator==(const itemid_t &other) const {
  return (type == other.type && location == other.location);
}

bool itemid_t::operator!=(const itemid_t &other) const {
  return !(*this == other);
}

void itemid_t::operator=(const itemid_t &other) {
  this->valid = other.valid;
  this->type = other.type;
  this->location = other.location;
  assert(*this == other);
  assert(this->valid);
}

void itemid_t::init() {
  valid = false;
  location = 0;
  next = NULL;
}

int get_thdid_from_txnid(uint64_t txnid) { return txnid % g_thread_cnt; }

uint64_t get_part_id(void *addr) {
  return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt;
}

uint64_t key_to_part(uint64_t key) {
  if (g_part_alloc)
    return key % g_part_cnt;
  else
    return 0;
}

uint64_t merge_idx_key(uint64_t key_cnt, uint64_t *keys) {
  uint64_t len = 64 / key_cnt;
  uint64_t key = 0;
  for (uint32_t i = 0; i < len; i++) {
    assert(keys[i] < (1UL << len));
    key = (key << len) | keys[i];
  }
  return key;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2) {
  assert(key1 < (1UL << 32) && key2 < (1UL << 32));
  return key1 << 32 | key2;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3) {
  assert(key1 < (1 << 21) && key2 < (1 << 21) && key3 < (1 << 21));
  return key1 << 42 | key2 << 21 | key3;
}

/****************************************************/
// Global Clock!
/****************************************************/
/*
inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
        ret = (uint64_t) ((double)ret / CPU_FREQ);
#else
        timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

inline uint64_t get_sys_clock() {
#ifndef NOGRAPHITE
        static volatile uint64_t fake_clock = 0;
        if (warmup_finish)
                return CarbonGetTime();   // in ns
        else {
                return ATOM_ADD_FETCH(fake_clock, 100);
        }
#else
        if (TIME_ENABLE)
                return get_server_clock();
        return 0;
#endif
}
*/
void myrand::init(uint64_t seed) { this->seed = seed; }

uint64_t myrand::next() {
  seed = (seed * 1103515247UL + 12345UL) % (1UL << 63);
  return (seed / 65537) % RAND_MAX;
}

string get_workload_file_name(const string & base_file_name, uint32_t thread_id) {
  string output = base_file_name;
  output += "/core_";
  output += to_string(thread_id);
  output += ".dat";
  return output;
}

string get_benchmark_path(const string & base, bool partitioned) {
  string path = base;
  path += "/";
  path += string(g_benchmark);
  path += "-";
  path += string(g_benchmark_tag);
  path += "-";
  if(g_benchmark_tag2 != nullptr) {
    path += string(g_benchmark_tag);
    path += "-";
  }
  path += "c";
  path += to_string(g_thread_cnt);
  path += "-";
  if(partitioned) {
    path += "partitioner-u";
    path += to_string(g_ufactor);
  } else {
    path += "raw";
  }
  return path;
}

void check_and_init_variables() {
  assert(g_benchmark != nullptr);
  assert((g_task_type == PARTITION_DATA || g_task_type == PARTITION_CONFLICT ||
          g_task_type == EXECUTE_PARTITIONED)
             ? g_ufactor != -1
             : true);

  if (strcmp(g_benchmark, "ycsb") == 0) {
    assert(g_benchmark_tag != nullptr);
    assert(g_benchmark_tag2 != nullptr);

    if (strcmp(g_benchmark_tag, "low") == 0) {
      g_zipf_theta = 0;
      g_read_perc = 0.9;
    } else if (strcmp(g_benchmark_tag, "medium") == 0) {
      g_zipf_theta = 0.8;
      g_read_perc = 0.9;
    } else if (strcmp(g_benchmark_tag, "high") == 0) {
      g_zipf_theta = 0.99;
      g_read_perc = 0.5;
    } else {
      assert(false);
    }
    g_size = g_size_factor * 1024;
    g_size_per_thread = g_size / g_thread_cnt;

    if (strcmp(g_benchmark_tag2, "sp-plc") == 0) {
      g_perc_multi_part = 0;
      g_part_cnt = g_thread_cnt / 2;
    } else if (strcmp(g_benchmark_tag2, "sp-pec") == 0) {
      g_perc_multi_part = 0;
      g_part_cnt = g_thread_cnt;
    } else if (strcmp(g_benchmark_tag2, "sp-pgc") == 0) {
      g_perc_multi_part = 0;
      g_part_cnt = g_thread_cnt * 4;
    } else if (strncmp(g_benchmark_tag2, "sp-custom-", 10) == 0) {
      g_perc_multi_part = 0;
      g_part_cnt = static_cast<uint32_t>(atoi(&g_benchmark_tag2[10]));
      assert(g_part_cnt > 0);
    } else {
      g_perc_multi_part = 1;
      if (strncmp(g_benchmark_tag2, "mp-custom-lc-", 13) == 0) {
        g_remote_perc = 0.5;
        g_part_cnt = static_cast<uint32_t>(atoi(&g_benchmark_tag2[13]));
        g_remote_partitions = static_cast<uint32_t>((double)0.1 * (double)g_part_cnt);
      } else if (strncmp(g_benchmark_tag2, "mp-custom-mc-", 13) == 0) {
        g_remote_perc = 0.5;
        g_part_cnt = static_cast<uint32_t>(atoi(&g_benchmark_tag2[13]));
        g_remote_partitions = static_cast<uint32_t>((double)0.5 * (double)g_part_cnt);
      } else if (strncmp(g_benchmark_tag2, "mp-custom-hc-", 13) == 0) {
        g_remote_perc = 0.5;
        g_part_cnt = static_cast<uint32_t>(atoi(&g_benchmark_tag2[13]));
        g_remote_partitions = static_cast<uint32_t>((double)0.9 * (double)g_part_cnt);
      } else {
        assert(false);
      }
      g_local_partitions = g_part_cnt / g_thread_cnt;
    }

  } else if (strcmp(g_benchmark, "tpcc") == 0) {
    assert(g_benchmark_tag != nullptr);
    if (strncmp(g_benchmark_tag, "wh", 2) == 0) {
      g_num_wh = static_cast<uint32_t>(atoi(&g_benchmark_tag[2]));
    } else {
      g_num_wh = g_thread_cnt;
    }
    g_size = g_size_factor * 1024;
    g_size_per_thread = g_size / g_thread_cnt;
  } else {
    assert(false);
  }
}


void ensure_folder_exists(string folder_path) {
  string cmd = "mkdir -p " + folder_path;
  if (system(cmd.c_str())) { printf("Folder %s created!", folder_path.c_str()); }
}