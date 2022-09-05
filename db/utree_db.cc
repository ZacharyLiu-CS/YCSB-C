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
  value_ptr = bt_->search(key_content);

  if (value_ptr == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }

  std::string value;
  engine_ptr_->read((NKV::PmemAddress)value_ptr, value);
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
  engine_ptr_->read((NKV::PmemAddress)value_ptr, value);

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
  NKV::PmemAddress addr;
  engine_ptr_->append(addr, value.data(), value.size());

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
  printStats();
  delete bt_;
  delete engine_ptr_;
}

} // namespace ycsbc
