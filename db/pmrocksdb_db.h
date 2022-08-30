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
  PMRocksDB(const char* dbfilename);
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

  ~PMRocksDB();

  private:
  rocksdb::DB* db_;
  rocksdb::Options options;
  std::atomic<unsigned> no_found_;

}; //end of YCSB
} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H
