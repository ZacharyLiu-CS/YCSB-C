//
//  utree_db.cc
//  YCSB-C
//
//  Created by zhenliu on 08/05/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "db/utree_db.h"
#include "config_reader.h"
#include "core/core_workload.h"
#include "db.h"
#include <chrono>

#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {

UTree::UTree(const char *dbfilename) : no_found_(0) {

  if (file_exists(dbfilename)) {
    mkdir(dbfilename, 0700);
  }

  ConfigReader config_reader = ConfigReader();
  utree_config *uc =
      static_cast<utree_config *>(config_reader.get_config("utree").get());
  bt_ = new btree(dbfilename, uc->db_size_);

  neopmkv_config *nc =
      static_cast<neopmkv_config *>(config_reader.get_config("neopmkv").get());
  engine_config_.chunk_size = nc->chunk_size_;
  engine_config_.engine_capacity = nc->db_size_;
  std::string db_path = dbfilename;
  db_path.append("/pmemlog");
  strcpy(engine_config_.engine_path, db_path.c_str());
  NKV::PmemEngine::open(engine_config_, &engine_ptr_);
}

Status UTree::Read(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) {
  char *value_ptr = nullptr;
  int64_t key_content = CoreWorkload::GetIntFromKey(key);

  auto t0 = Time::now();
  value_ptr = bt_->search(key_content);
  auto t1 = Time::now();
  index_read_count_.fetch_add(1);
  index_read_latency_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

  if (value_ptr == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }

  std::string value;
  auto t2 = Time::now();
  engine_ptr_->read((NKV::PmemAddress)value_ptr, value);
  auto t3 = Time::now();
  pmem_read_count_.fetch_add(1);
  pmem_read_latency_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count());

  if (fields != nullptr) {
    DeserializeRowFilter(result, value, *fields);
  } else {
    DeserializeRow(result, value);
  }

  return Status::kOK;
}

Status UTree::Scan(const std::string &table, const std::string &key, int len,
                   const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) {

  return Status::kOK;
}

Status UTree::Insert(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) {
  std::string value;
  SerializeRow(values, value);

  int64_t key_content = CoreWorkload::GetIntFromKey(key);

  NKV::PmemAddress addr;
  engine_ptr_->append(addr, value.data(), value.size());
  bt_->insert(key_content, (char *)addr);

  return Status::kOK;
}

Status UTree::Update(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) {
  // first read values from db
  char *value_ptr = nullptr;
  int64_t key_content = CoreWorkload::GetIntFromKey(key);
  value_ptr = bt_->search(key_content);
  if (value_ptr == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }
  // then update the specific field
  std::string value;
  auto t2 = Time::now();
  engine_ptr_->read((NKV::PmemAddress)value_ptr, value);
  auto t3 = Time::now();
  pmem_read_count_.fetch_add(1);
  pmem_read_latency_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count());

  auto t0 = Time::now();

  std::vector<KVPair> current_values;
  DeserializeRow(current_values, value);
  for (auto &new_field : values) {
    bool found = false;
    for (auto &current_field : current_values) {
      if (current_field.first == new_field.first) {
        found = true;
        current_field.second = new_field.second;
        break;
      }
    }
    if (found == false) {
      break;
    }
  }

  value.clear();
  SerializeRow(current_values, value);

  auto t1 = Time::now();
  update_parse_count_.fetch_add(1);
  update_parse_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

  auto t4 = Time::now();
  NKV::PmemAddress addr;
  engine_ptr_->append(addr, value.data(), value.size());
  auto t5 = Time::now();
  pmem_write_count_.fetch_add(1);
  pmem_write_latency_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t5 - t4).count());

  bt_->insert(key_content, (char *)addr);
  return Status::kOK;
}

Status UTree::Delete(const std::string &table, const std::string &key) {
  int64_t key_content = CoreWorkload::GetIntFromKey(key);
  bt_->remove(key_content);
  return Status::kOK;
}

void UTree::printStats() {
  std::cout << "print utree statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

UTree::~UTree() {
  if (index_read_count_.load() != 0) {
    std::cout << "Read index count: " << index_read_count_.load()
              << " Avarage latency (nanoseconds): "
              << index_read_latency_sum_.load() / index_read_count_.load()
              << std::endl;
  }
  if (pmem_read_count_.load() != 0) {
    std::cout << "Read pmem count: " << pmem_read_count_.load()
              << " Avarage latency (nanoseconds): "
              << pmem_read_latency_sum_.load() / pmem_read_count_.load()
              << std::endl;
  }
  if (pmem_write_count_.load() != 0) {
    std::cout << "Write pmem count: " << pmem_write_count_.load()
              << " Avarage latency (nanoseconds): "
              << pmem_write_latency_sum_.load() / pmem_write_count_.load()
              << std::endl;
  }
  if (update_parse_count_.load() != 0) {
    std::cout << "Update parse count: " << update_parse_count_.load()
              << " Avarage latency (nanoseconds): "
              << update_parse_sum_.load() / update_parse_count_.load()
              << std::endl;
  }
  printStats();
  delete bt_;
  delete engine_ptr_;
}

} // namespace ycsbc
