//
//  neopmkv_db.h
//  PROJECT neopmkv_db
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <atomic>
#include <bits/stdint-uintn.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "core/config_reader.h"
#include "core/core_workload.h"
#include "core/db.h"
#include "neopmkv.h"

namespace ycsbc {

class NEOPMKV : public DB {
public:
  NEOPMKV(const char *dbfilename);

  // init the schema
  uint64_t CreateSchema(std::string schema_name, size_t field_count,
                        size_t field_len, bool encoding_by_row) override {
    std::vector<NKV::SchemaField> fields;
    field_count_ = field_count;
    field_len_ = field_len;
    for (auto i = 0; i < field_count; i++) {
      std::string field_name = std::string("field").append(std::to_string(i));
      // std::cout << "field name size: " << field_name.size() << std::endl;
      fields.push_back(
          NKV::SchemaField(NKV::FieldType::STRING, field_name, field_len));
    }
    return neopmkv_->CreateSchema(fields, 0, schema_name);
  }

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields,
              std::vector<KVPair> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields,
              std::vector<std::vector<KVPair>> &result);

  Status Insert(const std::string &table, const std::string &key,
                std::vector<KVPair> &values);

  Status Update(const std::string &table, const std::string &key,
                std::vector<KVPair> &values);

  Status Delete(const std::string &table, const std::string &key);

  void printStats();

  inline uint64_t GetIntFromField(const std::string &field_name) {
    return std::stoull(field_name.substr(5, -1));
  }
  ~NEOPMKV();

  inline void GetFirstIntElementToVec(
      const std::vector<std::pair<std::string, std::string>> &pairs,
      std::vector<uint32_t> &items) {
    auto firstElement = [&](const std::pair<std::string, std::string> &p) {
      return this->GetIntFromField(p.first);
    };

    // Use std::transform() to convert the vector of pairs to a vector of
    // integers.
    std::transform(pairs.begin(), pairs.end(), std::back_inserter(items),
                   firstElement);
  }
  inline void GetSecondElementToVec(
      const std::vector<std::pair<std::string, std::string>> &pairs,
      std::vector<std::string> &items) {
    auto secondElement = [](const std::pair<std::string, std::string> &p) {
      return p.second;
    };

    // Use std::transform() to convert the vector of pairs to a vector of
    // integers.
    std::transform(pairs.begin(), pairs.end(), std::back_inserter(items),
                   secondElement);
  }

private:
  NKV::NeoPMKV *neopmkv_ = nullptr;
  std::atomic<unsigned> no_found_;
  bool enable_schema_aware_ = true;
  size_t field_count_ = 0;
  size_t field_len_ = 0;
  typedef std::chrono::high_resolution_clock Time;
  std::atomic<uint64_t> update_ser_count_{0};
  std::atomic<uint64_t> update_ser_sum_{0};
  std::atomic<uint64_t> update_des_count_{0};
  std::atomic<uint64_t> update_des_sum_{0};

  std::atomic<uint64_t> read_des_count_{0};
  std::atomic<uint64_t> read_des_sum_{0};
};
} // namespace ycsbc
