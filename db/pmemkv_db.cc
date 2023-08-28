//
//  pmemkv_db.cc
//  YCSB-C
//
//  Created by zhenliu on 15/06/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include <iostream>
#include <libpmemkv.h>
#include <libpmemkv.hpp>
#include <mutex>
#include <vector>

#include "core/config_reader.h"
#include "db.h"
#include "pmemkv_db.h"

#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    std::cerr << db_->errormsg() << std::endl;                                 \
    exit(0);                                                                   \
  } while (0)

using namespace pmem::kv;

namespace ycsbc {

PmemKV::PmemKV(const char *dbfilename) : no_found_(0) {
  ConfigReader config_reader = ConfigReader();
  pmemkv_config *pc =
      static_cast<pmemkv_config *>(config_reader.get_config("pmemkv").get());

  std::string mkdir_cmd = std::string("mkdir -p ") + std::string(dbfilename);
  int res = system(mkdir_cmd.c_str());
  std::string file_path(dbfilename);
  file_path = file_path + "/" + pc->engine_type_;
  // assign the option to cfg
  cfg_.put_path(file_path);
  cfg_.put_create_if_missing(pc->create_if_missing_);
  cfg_.put_size(pc->db_size_);
  db_ = new pmem::kv::db();
  status s = db_->open(pc->engine_type_, std::move(cfg_));
  if (s != status::OK) {
    LOGOUT("init pmemkv failed!");
  }
}

Status PmemKV::Read(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {
  // encoding by column
  if (encoding_by_row_ == false) {
    // read all field
    if (fields == nullptr) {
      for (auto i = 0; i < field_count_; i++) {
        std::string field_key = key;
        field_key.append("field").append(std::to_string(i));
        std::string field_value;
        status s = db_->get(field_key, &field_value);
        result.push_back({field_key, field_value});
      }
      return Status::kOK;
    }
    // read only some fields
    for (auto &field_name : *fields) {
      std::string field_value;
      std::string field_key = key;
      field_key.append(field_name);
      status s = db_->get(field_key, &field_value);

      result.push_back({field_key, field_value});
    }
    return Status::kOK;
  }

  // encoding by row
  std::lock_guard<std::mutex> guard(mutex_);
  std::string value;
  status s = db_->get(key, &value);
  if (s == status::NOT_FOUND) {
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

Status PmemKV::Scan(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {

  std::lock_guard<std::mutex> guard(mutex_);
  auto res_iter = db_->new_read_iterator();
  if (!res_iter.is_ok()) {
    LOGOUT("Error new read iterator in pmemkv");
  }
  // encoding by column

  auto &iter = res_iter.get_value();
  iter.seek(key);
  if (encoding_by_row_ == false) {
    // read all fields
    if (fields == nullptr) {
      for (int i = 0; i < len; i++) {
        result.push_back(std::vector<KVPair>());
        std::vector<KVPair> &values = result.back();
        for (auto i = 0; i < field_count_; i++) {
          pmem::kv::result<string_view> res_val = iter.read_range();
          std::string field_value = res_val.get_value().data();
          std::string field_key =
              std::string("field").append(std::to_string(i));
          values.push_back({field_key, field_value});
          iter.next();
        }
      }
      return Status::kOK;
    }
    // read only some fields
    for (int i = 0; i < len; i++) {
      result.push_back(std::vector<KVPair>());
      std::vector<KVPair> &values = result.back();
      for (auto i = 0; i < field_count_; i++) {
        pmem::kv::result<string_view> res_val = iter.read_range();
        std::string field_value = res_val.get_value().data();

        std::string field_key = std::string("field").append(std::to_string(i));
        if (std::find(fields->begin(), fields->end(), field_key) !=
            fields->end()) {
          values.push_back({field_key, field_value});
        }
        iter.next();
      }
    }
    return Status::kOK;
  }

  for (int i = 0; iter.next() == status::OK && i < len; i++) {
    pmem::kv::result<string_view> res_val = iter.read_range();
    if (!res_val.is_ok()) {
      LOGOUT("Error read value from iterator of pmemkv");
    }
    // get value from iter
    std::string value = res_val.get_value().data();
    // generate a new vector<KVPair> value
    result.push_back(std::vector<KVPair>());
    auto values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, value, *fields);
    } else {
      DeserializeRow(values, value);
    }
  }
  return Status::kOK;
}

Status PmemKV::Insert(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {

  std::lock_guard<std::mutex> guard(mutex_);
  if (encoding_by_row_ == false) {
    // encoding by column
    for (auto &[field_name, field_value] : values) {
      std::string field_key = key + field_name;
      status s = db_->put(field_key, field_value);
    }
    return Status::kOK;
  }

  std::string value;
  SerializeRow(values, value);

  status s = db_->put(key, value);
  if (s != status::OK) {
    LOGOUT("Error insert data to pmemkv!");
  }
  return Status::kOK;
}

Status PmemKV::Update(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {

  std::lock_guard<std::mutex> guard(mutex_);
  // encoding by column
  if (encoding_by_row_ == false) {
    for (auto &[field_name, field_value] : values) {
      std::string field_key = key + field_name;
      status s = db_->put(field_key, field_value);
    }
    return Status::kOK;
  }

  // first read values from db
  std::string value;
  status s = db_->get(key, &value);
  if (s == status::NOT_FOUND) {
    no_found_++;
    return Status::kErrorNoData;
  }
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
  s = db_->put(key, value);
  if (s != status::OK) {
    LOGOUT("Error update data from pmemkv");
  }
  return Status::kOK;
}

Status PmemKV::Delete(const std::string &table, const std::string &key) {

  std::lock_guard<std::mutex> guard(mutex_);
  // if encoding by column
  if (encoding_by_row_ == false) {
    for (auto i = 0; i < field_count_; i++) {
      std::string field_key = key;
      field_key.append("field").append(std::to_string(i));
      status s = db_->remove(field_key);
    }
    return Status::kOK;
  }

  // encoding by row
  status s = db_->remove(key);
  if (s != status::OK) {
    LOGOUT("Error delete data from pmemkv!");
  }
  return Status::kOK;
}

void PmemKV::printStats() {
  std::cout << "print pmemkv statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

PmemKV::~PmemKV() {
  printStats();
  delete db_;
}

} // namespace ycsbc
