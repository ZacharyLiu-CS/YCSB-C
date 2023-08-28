//
//  rocksdb_db.h
//  YCSB-C
//
//  Created by zhenliu on 18/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#ifndef YCSB_C_ROCKSDB_DB_H
#define YCSB_C_ROCKSDB_DB_H

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "core/config_reader.h"
#include "core/db.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace ycsbc {

class PMRocksDB : public DB {
public:
  PMRocksDB(const char *dbfilename);
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

  void printStats();

  ~PMRocksDB();

private:
  rocksdb::DB *db_;
  rocksdb::Options options;
  bool encoding_by_row_;
  size_t field_count_;
  std::atomic<unsigned> no_found_;

}; // end of YCSB
} // namespace ycsbc

#endif // YCSB_C_ROCKSDB_DB_H
