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


#include "core/db.h"
#include "core/config_reader.h"
#include "listdb/db_client.h"
#include "listdb/listdb.h"

namespace ycsbc {

class LISTDB : public DB {
  public:
  LISTDB(const char* dbfilename);
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

  inline int64_t getIntFromKey(const char* key)
  {
    int64_t key_content = 0;
    sscanf(key, "%lduser0", &key_content);
    return key_content;
  }
  inline int file_exists(char const *file) { return access(file, F_OK); }
  ~LISTDB();

  private:
  ListDB* db_ = nullptr; 
  DBClient* client_ = nullptr;
  std::atomic<unsigned> no_found_;

}; //end of LISTDB

} //ycsbc


