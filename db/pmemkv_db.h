//
//  pmemkv_db.h
//  YCSB-C
//
//  Created by zhenliu on 15/06/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#ifndef YCSB_C_PMEMKV_DB_H
#define YCSB_C_PMEMKV_DB_H

#include <atomic>
#include <cstdint>
#include <libpmemkv.hpp>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "core/config_reader.h"
#include "core/db.h"

namespace ycsbc {

class PmemKV : public DB {
public:
  PmemKV(const char *dbfilename);
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

  ~PmemKV();

private:
  pmem::kv::db *db_;
  pmem::kv::config cfg_;
  bool encoding_by_row_;
  size_t field_count_;
  std::atomic<unsigned> no_found_;
  std::mutex mutex_;

}; // end of PmemKV

} // namespace ycsbc

#endif // YCSB_C_PmemKV_DB_H
