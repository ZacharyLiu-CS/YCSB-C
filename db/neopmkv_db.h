//
//  neopmkv_db.h
//  PROJECT neopmkv_db
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "core/db.h"
#include "core/config_reader.h"
#include "neopmkv.h"

namespace ycsbc {

class NEOPMKV : public DB {
  public:
  NEOPMKV(const char* dbfilename);
  
  // init the schema
  uint64_t CreateSchema(std::string schema_name ,size_t field_count, size_t field_len) {
    std::vector<NKV::SchemaField> fields;
    for(auto i = 0; i< field_count; i++){
        std::string field_name = std::string("field").append(std::to_string(i));
        std::string field_content = std::string("content").append(std::to_string(i));

        fields.push_back(NKV::SchemaField(NKV::FieldType::STRING, field_name, field_name.size()));
        fields.push_back(NKV::SchemaField(NKV::FieldType::STRING,field_content, field_len));
    }
    return neopmkv_->createSchema( fields, 0, schema_name);
  }

  Status Read(const std::string& table, const std::string& key,
      const std::vector<std::string>* fields,
      std::vector<KVPair>& result);

  Status Scan(const std::string& table, const std::string& key,
      int len, const std::vector<std::string>* fields,
      std::vector<std::vector<KVPair>>& result);

  Status Insert(const std::string& table, const std::string& key,
      std::vector<KVPair>& values);

  Status Update(const std::string& table, const std::string& key,
      std::vector<KVPair>& values);

  Status Delete(const std::string& table, const std::string& key);

  void printStats();

  ~NEOPMKV();

 
  private:
  NKV::NeoPMKV * neopmkv_ = nullptr;
  std::atomic<unsigned> no_found_;

}; 
} // end of ycsbc

