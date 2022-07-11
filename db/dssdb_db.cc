//
//  dssdb_db.cc
//  YCSB-C
//
//  Created by zhenliu on 07/11/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "dssdb_db.h"
#include "include/kv_engine.h"

#include <iostream>
#include <iterator>
#include <vector>

#define LOGOUT(msg)                   \
  do{                                 \
    std::cerr << msg << std::endl;    \
    exit(0);                          \
  } while (0)

namespace ycsbc {

DssDB::DssDB(const std::string& host, const std::string& port )
    : no_found_(0)
{
  kv_impl_ = new kv::LocalEngine();
  if (kv_impl_ == nullptr){
    LOGOUT("init polarkv failed!");
    assert(kv_impl_);
  }
  kv_impl_->start(host, port);
}

Status DssDB::Read(const std::string& table, const std::string& key,
                     const std::vector<std::string>* fields,
                     std::vector<KVPair>& result)
{
  std::string value;
  auto s = kv_impl_->read(key, value);
  if ( !s ) {
    no_found_++;
    return Status::kErrorNoData;
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, value, *fields);
  } else {
    DeserializeRow(result, value);
  }
  return Status::kOK;
}

Status DssDB::Scan(const std::string& table, const std::string& key, int len,
                  const std::vector<std::string>* fields,
                  std::vector<std::vector<KVPair>>& result)
{
    return Status::kOK;
}

Status DssDB::Insert(const std::string& table, const std::string& key,
                       std::vector<KVPair>& values)
{
  std::string value;
  SerializeRow(values, value);
  if(key.length()!=16)
    std::cerr << "YCSB Error: Key length not 16!" <<std::endl;

  auto s = kv_impl_->write(key, value);
  if (!s) {
    LOGOUT("Write Error!");
  }
  return Status::kOK;
}

Status DssDB::Update(const std::string& table, const std::string& key,
                       std::vector<KVPair>& values)
{
  // first read values from db
  std::string value;
  auto s = kv_impl_->read(key, value);
  if (!s) {
    no_found_++;
    return Status::kErrorNoData;
  }
  // then update the specific field
  std::vector<KVPair> current_values;
  DeserializeRow(current_values, value);
  for (auto& new_field : values) {
    bool found = false;
    for (auto& current_field : current_values) {
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
  s = kv_impl_->write(key, value);
  if (!s) {
    LOGOUT("Update Error!");
  }
  return Status::kOK;
}

Status DssDB::Delete(const std::string& table, const std::string& key)
{
  return Status::kOK;
}
void DssDB::close(){
  kv_impl_->stop();
}
DssDB::~DssDB()
{
  delete kv_impl_;
}

} //ycsbc
