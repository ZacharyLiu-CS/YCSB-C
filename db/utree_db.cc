//
//  utree_db.cc
//  YCSB-C
//
//  Created by zhenliu on 08/05/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "db/utree_db.h"

#define LOGOUT(msg)                \
  do {                             \
    std::cerr << msg << std::endl; \
    exit(0);                       \
  } while (0)

namespace ycsbc {



UTree::UTree(const char* dbfilename)
    : no_found_(0)
{

  if (file_exists(dbfilename)) {
    mkdir(dbfilename, 0700);
  }

  ConfigReader config_reader = ConfigReader();
  utree_config* uc = static_cast<utree_config*>(config_reader.get_config("utree").get());

  bt_ = new btree(dbfilename, uc->db_size_);

}

Status UTree::Read(const std::string& table, const std::string& key,
    const std::vector<std::string>* fields,
    std::vector<KVPair>& result)
{
  char* value = nullptr;
  int64_t key_content = getIntFromKey(key.c_str());
  value = bt_->search(key_content);

  if (value == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }

  return Status::kOK;
}

Status UTree::Scan(const std::string& table, const std::string& key, int len,
    const std::vector<std::string>* fields,
    std::vector<std::vector<KVPair>>& result)
{

  return Status::kOK;
}

Status UTree::Insert(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
  std::string value;
  SerializeRow(values, value);
  int64_t key_content = getIntFromKey(key.c_str());
  bt_->insert(key_content, value.data());
  return Status::kOK;
}

Status UTree::Update(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
  // first read values from db
  char* value_content = nullptr;
  int64_t key_content = getIntFromKey(key.c_str());
  value_content = bt_->search(key_content); 
  if (value_content == nullptr) {
    no_found_++;
    return Status::kErrorNoData;
  }
  // then update the specific field
  std::string value(value_content);

  bt_->insert(key_content, value.data());
  return Status::kOK;
}

Status UTree::Delete(const std::string& table, const std::string& key)
{
  int64_t key_content = getIntFromKey(key.c_str());
  bt_->remove(key_content);
  return Status::kOK;
}

void UTree::printStats()
{
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

UTree::~UTree()
{
  std::cout << "print fastfair statistics: " << std::endl;
  printStats();
  delete bt_;
}

} //ycsbc
