//
//  fastfair_db.cc
//  YCSB-C
//
//  Created by zhenliu on 08/05/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "db/fastfair_db.h"
#include "core/core_workload.h"

#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {

FastFair::FastFair(const char *dbfilename) : no_found_(0) {
  ConfigReader config_reader = ConfigReader();
  fastfair_config *ffc = static_cast<fastfair_config *>(
      config_reader.get_config("fastfair").get());
  bt_ = TOID_NULL(btree);

  if (file_exists(dbfilename)) {
    mkdir(dbfilename, 0700);
  }

  char full_path[100];
  sprintf(full_path, "%s/btree", dbfilename);
  if (!file_exists(full_path)) {
    remove(full_path);
  }
  pop_ = pmemobj_create(full_path, "btree", ffc->db_size_, 0666);
  bt_ = POBJ_ROOT(pop_, btree);
  D_RW(bt_)->constructor(pop_);
  if (pop_ == nullptr) {
    LOGOUT("Init the fastfair btree failed!");
  }
  neopmkv_config *nc =
      static_cast<neopmkv_config *>(config_reader.get_config("neopmkv").get());
  engine_config_.chunk_size = nc->chunk_size_;
  engine_config_.engine_capacity = nc->db_size_;
  std::string db_path = dbfilename;
  db_path.append("/pmemlog");
  strcpy(engine_config_.engine_path, db_path.c_str());
  NKV::PmemEngine::open(engine_config_, &engine_ptr_);
}

Status FastFair::Read(const std::string &table, const std::string &key,
                      const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
  char *value_ptr = nullptr;
  int64_t key_content = CoreWorkload::GetIntFromKey(key);
  value_ptr = D_RW(bt_)->btree_search(key_content);

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

Status FastFair::Scan(const std::string &table, const std::string &key, int len,
                      const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
  // int64_t start_key = CoreWorkload::GetIntFromKey(key);
  // uint64_t valueptr_list[len];
  // // std::cout << "Scan start key: " << start_key << std::endl;
  // D_RW(bt_)->btree_search_range(start_key, INT32_MAX, valueptr_list, len);
  // for (auto i = 0; i < len; i++) {
  //   std::string value;
  //   if (valueptr_list[i] == (unsigned long)NULL) {
  //     continue;
  //   }
  //   value.clear();
  //   engine_ptr_->read((NKV::PmemAddress)valueptr_list[i], value);
  //   // std::cout << "Key: " << valueptr_list[i] << " Value: " << value << std::endl;
  //   result.push_back(std::vector<KVPair>());
  //   std::vector<KVPair> &values = result.back();
  // }
  return Status::kOK;
}

Status FastFair::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values) {
  std::string value;
  SerializeRow(values, value);

  NKV::PmemAddress addr;
  engine_ptr_->append(addr, value.data(), value.size());
  int64_t key_content = CoreWorkload::GetIntFromKey(key);
  D_RW(bt_)->btree_insert(key_content, (char *)addr);

  // std::cout << "Insert key: "<< key_content << " Ptr: " << addr << std::endl;
  return Status::kOK;
}

Status FastFair::Update(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values) {
  char *value_ptr = nullptr;
  int64_t key_content = CoreWorkload::GetIntFromKey(key);
  value_ptr = D_RW(bt_)->btree_search(key_content);
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

  D_RW(bt_)->btree_insert(key_content, (char *)addr);
  return Status::kOK;
}

Status FastFair::Delete(const std::string &table, const std::string &key) {
  int64_t key_content = CoreWorkload::GetIntFromKey(key);
  D_RW(bt_)->btree_delete(key_content);
  return Status::kOK;
}

void FastFair::printStats() {
  std::cout << "print fastfair statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

FastFair::~FastFair() {
  printStats();
  pmemobj_close(pop_);
  delete engine_ptr_;
}

} // namespace ycsbc
