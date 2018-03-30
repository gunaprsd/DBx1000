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

int get_thdid_from_txnid(uint64_t txnid) { return txnid % FLAGS_threads; }

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


string get_workload_file_name(const string & base_file_name, uint32_t thread_id) {
  string output = base_file_name;
  output += "/core_";
  output += to_string(thread_id);
  output += ".dat";
  return output;
}

void ensure_folder_exists(string folder_path) {
  string cmd = "mkdir -p " + folder_path;
  if (system(cmd.c_str())) { printf("Folder %s created!", folder_path.c_str()); }
}