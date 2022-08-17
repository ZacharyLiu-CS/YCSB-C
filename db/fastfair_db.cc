//
//  fastfair_db.cc
//  YCSB-C
//
//  Created by zhenliu on 08/05/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "db/fastfair_db.h"

#define LOGOUT(msg)                \
  do {                             \
    std::cerr << msg << std::endl; \
    exit(0);                       \
  } while (0)

namespace ycsbc {

inline int64_t getIntFromKey(char* key)
{
  int64_t key_content = 0;
  sscanf(key, "%lduser0", &key_content);
  return key_content;
}

FastFair::FastFair(const char* dbfilename)
    : no_found_(0)
{
  Config_Reader config_reader = Config_Reader();
  fastfair_config* ffc = static_cast<fastfair_config*>(config_reader.get_config("fastfair").get());
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
}

Status FastFair::Read(const std::string& table, const std::string& key,
    const std::vector<std::string>* fields,
    std::vector<KVPair>& result)
{
  char* value = nullptr;
  int64_t key_content = getIntFromKey(key.c_str());
  value = D_RW(bt_)->btree_search(key_content);

  if (value == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }

  return Status::kOK;
}

Status FastFair::Scan(const std::string& table, const std::string& key, int len,
    const std::vector<std::string>* fields,
    std::vector<std::vector<KVPair>>& result)
{
  int64_t start_key = getIntFromKey(key.c_str());
  unsigned long buf[len];
  D_RW(bt_)->btree_search_range(start_key, INT32_MAX, buf, len);
  return Status::kOK;
}

Status FastFair::Insert(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
  std::string value;
  SerializeRow(values, value);
  int64_t key_content = getIntFromKey(key.c_str());
  D_RW(bt_)->btree_insert(key_content, const_cast<char*>(value.c_str()));

  return Status::kOK;
}

Status FastFair::Update(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
  // first read values from db
  char* value_content = nullptr;
  int64_t key_content = getIntFromKey(key.c_str());
  value_content = D_RW(bt_)->btree_search(key_content);
  if (value_content == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }
  // then update the specific field
  std::string value(value_content);

  D_RW(bt_)->btree_insert(key_content, const_cast<char*>(value.c_str()));
  return Status::kOK;
}

Status FastFair::Delete(const std::string& table, const std::string& key)
{
  int64_t key_content = getIntFromKey(key.c_str());
  D_RW(bt_)->btree_delete(key_content);
  return Status::kOK;
}

void FastFair::printStats()
{
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

FastFair::~FastFair()
{
  std::cout << "print fastfair statistics: " << std::endl;
  printStats();
  pmemobj_close(pop_);
}

} //ycsbc
