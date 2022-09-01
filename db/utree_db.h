//
//  utree_db.h
//  YCSB-C
//
//  Created by zhenliu on 08/05/2022.
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
#include "utree.h"
#include "pmem_log.h"
#include "pmem_engine.h"
#include "core/config_reader.h"


namespace ycsbc {

class UTree : public DB {
  public:
  UTree(const char* dbfilename);
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

  
  ~UTree();

  private:
  ::btree *bt_;
  std::atomic<unsigned> no_found_;
  NKV::PmemEngine * engine_ptr_;
  NKV::PmemEngineConfig engine_config_;

}; //end of utree

} //ycsbc

