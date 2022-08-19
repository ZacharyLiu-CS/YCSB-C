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
    std::cout << msg << std::endl;    \
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

  return Status::kOK;
}

Status DssDB::Scan(const std::string& table, const std::string& key, int len,
                  const std::vector<std::string>* fields,
                  std::vector<std::vector<KVPair>>& result)
{
    //do not support scan operation
    return Status::kOK;
}

Status DssDB::Insert(const std::string& table, const std::string& key,
                       std::vector<KVPair>& values)
{
  std::string value = values[0].second;
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

  value.clear();
  value = values[0].second;
  s = kv_impl_->write(key, value);
 
  return Status::kOK;
}

Status DssDB::Delete(const std::string& table, const std::string& key)
{
  return Status::kOK;
}
void DssDB::Close(){
  std::cout << "No found: " << no_found_  << std::endl; 
  kv_impl_->stop();
}
DssDB::~DssDB()
{
  delete kv_impl_;
}

} //ycsbc
