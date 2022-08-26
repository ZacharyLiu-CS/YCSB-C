//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

namespace ycsbc {
std::shared_ptr<DB> DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return std::make_shared<BasicDB>();
  } else if (props["dbname"] == "lock_stl") {
    return std::make_shared<LockStlDB>();
  } else if (props["dbname"] == "leveldb") {
    return std::make_shared<LevelDB>(props["dbpath"].c_str());
  } else if (props["dbname"] == "rocksdb") {
    return std::make_shared<RocksDB>(props["dbpath"].c_str());
  } else if (props["dbname"] == "pmemkv") {
    return std::make_shared<PmemKV>(props["dbpath"].c_str());
  } else if (props["dbname"] == "fastfair") {
    return std::make_shared<FastFair>(props["dbpath"].c_str());
  } else if (props["dbname"] == "neopmkv") {
    return std::make_shared<NEOPMKV>(props["dbpath"].c_str());
  } else if (props["dbname"] == "utree") {
    return std::make_shared<UTree>(props["dbpath"].c_str());
  } else

    return NULL;
}

} // namespace ycsbc
