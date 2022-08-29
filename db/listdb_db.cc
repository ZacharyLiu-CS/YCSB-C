//
//  listdb_db.cc
//  PROJECT listdb_db
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "db/listdb_db.h"

#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {


LISTDB::LISTDB(const char *dbfilename) : no_found_(0) {
  if (file_exists(dbfilename)) {
    mkdir(dbfilename, 0700);
  }
  ConfigReader config_reader = ConfigReader();
  listdb_config* lc = static_cast<listdb_config*>(config_reader.get_config("listdb").get());
  db_ = new ListDB();
  db_->Init(dbfilename, lc->pool_size_);
  client_ = new DBClient(db_, 0, 0);
}

Status LISTDB::Read(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {

  uint64_t value;
  bool res = client_->GetStringKV(key, &value);
  if (res == false) {
    no_found_++;
    return Status::kErrorNoData;
  }
  return Status::kOK;
}

Status LISTDB::Scan(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {

  return Status::kOK;
}

Status LISTDB::Insert(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
  std::string value;
  SerializeRow(values, value);
  client_->PutStringKV(key, value);
  return Status::kOK;
}

Status LISTDB::Update(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
  // first read values from db
  uint64_t value;
  bool res = client_->GetStringKV(key, &value);
  std::string new_value;
  SerializeRow(values, new_value);
  client_->PutStringKV(key, new_value);
  return Status::kOK;
}

Status LISTDB::Delete(const std::string &table, const std::string &key) {
  std::string null_value;
  client_->PutStringKV(key, null_value);
  return Status::kOK;
}

void LISTDB::printStats() {
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

LISTDB::~LISTDB() {
  std::cout << "print fastfair statistics: " << std::endl;
  printStats();
  delete client_;
  delete db_;
}

} // namespace ycsbc
