//
//  listdb_db.cc
//  PROJECT listdb_db
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "db/listdb_db.h"
#include "db.h"

#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {

inline std::string convert_valueptr_to_value(uint64_t& value_ptr) {
  size_t value_len = *((size_t *)value_ptr);
  value_ptr += sizeof(size_t);
  std::string value;
  value.assign((char*)value_ptr, value_len);
  return value;
}

LISTDB::LISTDB(const char *dbfilename) : no_found_(0) {
  if (file_exists(dbfilename)) {
    mkdir(dbfilename, 0700);
  }
  ConfigReader config_reader = ConfigReader();
  listdb_config *lc =
      static_cast<listdb_config *>(config_reader.get_config("listdb").get());
  db_ = new ListDB();
  db_->Init(dbfilename, lc->pool_size_);
  client_ = new DBClient(db_, 0, 0);
}

Status LISTDB::Read(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {

  uint64_t value_ptr;
  bool res = client_->GetStringKV(key, &value_ptr);
  if (res == false) {
    no_found_++;
    return Status::kErrorNoData;
  }
  std::string value = convert_valueptr_to_value(value_ptr);
  if (fields != nullptr) {
    DeserializeRowFilter(result, value, *fields);
  } else {
    DeserializeRow(result, value);
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
  uint64_t value_ptr;
  bool res = client_->GetStringKV(key, &value_ptr);
  if (res == false){
    no_found_++;
    return Status::kErrorNoData;
  }
  std::string value = convert_valueptr_to_value(value_ptr);

  // then update the specific field
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
  client_->PutStringKV(key, value);

  return Status::kOK;
}

Status LISTDB::Delete(const std::string &table, const std::string &key) {
  std::string null_value;
  client_->PutStringKV(key, null_value);
  return Status::kOK;
}

void LISTDB::printStats() {
  std::cout << "print listdb statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

LISTDB::~LISTDB() {
  printStats();
  delete client_;
  delete db_;
}

} // namespace ycsbc
