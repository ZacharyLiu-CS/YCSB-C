//
//  neopmkv_db.cc
//  PROJECT neopmkv_db
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "neopmkv_db.h"
#include "core/core_workload.h"
#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {

NEOPMKV::NEOPMKV(const char *dbfilename) : no_found_(0) {
  std::string clean_cmd = std::string("rm -rf ") + std::string(dbfilename);
  int res = system(clean_cmd.c_str());

  std::string mkdir_cmd = std::string("mkdir -p ") + std::string(dbfilename);
  res = system(mkdir_cmd.c_str());

  ConfigReader config_reader = ConfigReader();
  neopmkv_config *nc =
      static_cast<neopmkv_config *>(config_reader.get_config("neopmkv").get());

  if (neopmkv_ == nullptr) {
    neopmkv_ =
        new NKV::NeoPMKV(dbfilename, nc->chunk_size_, nc->db_size_,
                         nc->enable_pbrb_, nc->async_pbrb_, nc->max_page_num_);
  }
  if (neopmkv_ == nullptr) {
    LOGOUT("init neopmkv failed!");
  }
}

Status NEOPMKV::Read(const std::string &table, const std::string &key,
                     const std::vector<std::string> *fields,
                     std::vector<KVPair> &result) {
  std::string value;
  NKV::SchemaId schemaId = key[3] - '0';

  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  auto s = neopmkv_->get(read_key, value);
  if (s == false) {
    no_found_++;
    return Status::kErrorNoData;
  }
  // if (fields != nullptr) {
  //   DeserializeRowFilter(result, value, *fields);
  // } else {
  //   DeserializeRow(result, value);
  // }
  return Status::kOK;
}

Status NEOPMKV::Scan(const std::string &table, const std::string &key, int len,
                     const std::vector<std::string> *fields,
                     std::vector<std::vector<KVPair>> &result) {
  std::vector<std::string> read_value;
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  uint64_t key_content = CoreWorkload::GetIntFromKey(key);
  neopmkv_->scan(read_key, read_value, len);
  for (auto &v : read_value) {
    result.push_back(std::vector<KVPair>());
    std::vector<KVPair> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, v, *fields);
    } else {
      DeserializeRow(values, v);
    }
  }
  return Status::kOK;
}

Status NEOPMKV::Insert(const std::string &table, const std::string &key,
                       std::vector<KVPair> &values) {
  std::string value;
  SerializeRow(values, value);
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  auto s = neopmkv_->put(read_key, value);
  return Status::kOK;
}

Status NEOPMKV::Update(const std::string &table, const std::string &key,
                       std::vector<KVPair> &values) {
  // first read values from db
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));

  std::string value;
  auto s = neopmkv_->update(read_key, values);
  if (s == false) {
    no_found_++;
    return Status::kErrorNoData;
  }
  return Status::kOK;
}

Status NEOPMKV::Delete(const std::string &table, const std::string &key) {
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));

  auto s = neopmkv_->remove(read_key);

  return Status::kOK;
}

void NEOPMKV::printStats() {
  std::cout << "print neopmkv statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

NEOPMKV::~NEOPMKV() {
  printStats();
  delete neopmkv_;
}
} // namespace ycsbc
