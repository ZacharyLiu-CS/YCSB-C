//
//  dssdb_db.h
//  YCSB-C
//
//  Created by zhenliu on 07/11/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#ifndef YCSB_C_DSSDB_DB_H
#define YCSB_C_DSSDB_DB_H

#include <cstdint>
#include <string>
#include <vector>
#include <atomic>

#include "core/config_reader.h"
#include "core/db.h"
#include "include/kv_engine.h"

namespace ycsbc {

class DssDB : public DB {
  public:
  DssDB(const std::string &host, const std::string & port);
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

  void Close();


  ~DssDB();

  private:
  kv::LocalEngine *kv_impl_;
  std::atomic<unsigned> no_found_;

}; //end of DssDB

} //ycsbc

#endif //YCSB_C_LEVELDB_DB_H
