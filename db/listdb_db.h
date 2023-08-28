//
//  listdb_db.h
//  PROJECT listdb_db
//
//  Created by zhenliu on 24/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "core/config_reader.h"
#include "core/db.h"
#include "listdb/db_client.h"
#include "listdb/listdb.h"

namespace ycsbc {

class LISTDB : public DB {
public:
  LISTDB(const char *dbfilename);
  uint64_t CreateSchema(std::string schema_name, size_t field_count,
                        size_t field_len, bool encoding_by_row) override {
    field_count_ = field_count;
    encoding_by_row_ = encoding_by_row;
    std::cout << "encoding by row: " << encoding_by_row << std::endl;
    return 0;
  }
  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields,
              std::vector<KVPair> &result) override;

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields,
              std::vector<std::vector<KVPair>> &result) override;

  Status Insert(const std::string &table, const std::string &key,
                std::vector<KVPair> &values) override;

  Status Update(const std::string &table, const std::string &key,
                std::vector<KVPair> &values) override;

  Status Delete(const std::string &table, const std::string &key) override;

  void close();

  void printStats();

  inline int file_exists(char const *file) { return access(file, F_OK); }
  ~LISTDB();

private:
  ListDB *db_ = nullptr;
  DBClient *client_ = nullptr;
  bool encoding_by_row_;
  size_t field_count_;
  std::atomic<unsigned> no_found_;

}; // end of LISTDB

} // namespace ycsbc
