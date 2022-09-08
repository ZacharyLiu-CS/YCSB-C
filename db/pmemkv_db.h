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
#include <string>
#include <string_view>
#include <vector>
#include <mutex>

#include "core/config_reader.h"
#include "core/db.h"

namespace ycsbc {

class PmemKV : public DB {
  public:
  PmemKV(const char* dbfilename);
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

  void close();

  void printStats();

  ~PmemKV();

  private:
  pmem::kv::db* db_;
  pmem::kv::config cfg_;
  std::atomic<unsigned> no_found_;
  std::mutex mutex_;

}; //end of PmemKV

} //ycsbc

#endif //YCSB_C_PmemKV_DB_H
