//
//  rocksdb_db.cc
//  YCSB-C
//
//  Created by zhenliu on 18/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "rocksdb_db.h"

#include <iostream>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

using std::cerr;
using std::cout;
using std::endl;

#define LOGOUT(msg)                   \
  do{                                 \
    std::cerr << msg << std::endl;    \
    exit(0);                          \
  } while (0)

namespace ycsbc {

RocksDB::RocksDB(const char* dbfilename)
    : no_found_(0)
{
  ConfigReader config_reader = ConfigReader();
  db_config* dc = static_cast<db_config*>(config_reader.get_config("rocksdb").get());
  //create database if not exists
  options.create_if_missing = true;
  options.use_direct_reads = dc->enable_direct_io_;
  options.use_direct_io_for_flush_and_compaction = dc->enable_direct_io_;
  options.disable_auto_compactions = !dc->enable_compaction_;
  options.max_background_compactions = dc->thread_compaction_;
  options.max_background_jobs = dc->thread_compaction_ + 4;
  options.write_buffer_size = dc->memtable_size_;
  options.target_file_size_base = dc->sst_file_size_;
  rocksdb::BlockBasedTableOptions block_options;
  block_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(dc->bloom_bits_));
  block_options.block_cache = rocksdb::NewLRUCache(dc->block_cache_size_);

  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_options));
  options.statistics = rocksdb::CreateDBStatistics();

  rocksdb::Status s = rocksdb::DB::Open(options, dbfilename, &db_);
  if (!s.ok()) {
    LOGOUT("init rocksdb failed!");
  }
}

Status RocksDB::Read(const std::string& table, const std::string& key, const std::vector<std::string>* fields, std::vector<KVPair>& result)
{
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (s.IsNotFound()) {
    no_found_++;
    return Status::kErrorNoData;
  } else if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, value, *fields);
  } else {
    DeserializeRow(result, value);
  }
  return Status::kOK;
}

Status RocksDB::Scan(const std::string& table, const std::string& key, int len, const std::vector<std::string>* fields, std::vector<std::vector<KVPair>>& result)
{
  rocksdb::Iterator* iter = db_->NewIterator(rocksdb::ReadOptions());
  iter->Seek(key);
  iter->Seek(key);
  for (int i = 0; iter->Valid() && i < len; i++) {
    std::string value = iter->value().ToString();
    result.push_back(std::vector<KVPair>());
    std::vector<KVPair>& values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, value, *fields);
    } else {
      DeserializeRow(values, value);
    }
    iter->Next();
  }
  delete iter;
  return Status::kOK;

}

Status RocksDB::Insert(const std::string& table, const std::string& key, std::vector<KVPair>& values)
{
  std::string value;
  SerializeRow(values, value);

  rocksdb::Status s;
  s = db_->Put(rocksdb::WriteOptions(), key, value);
  if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  return Status::kOK;
}

Status RocksDB::Update(const std::string& table, const std::string& key, std::vector<KVPair>& values)
{
  // first read values from db
  std::string value;
  rocksdb::Status s;
  s = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (s.IsNotFound()) {
    no_found_++;
    return Status::kErrorNoData;
  } else if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  // then update the specific field
  std::vector<KVPair> current_values;
  DeserializeRow(current_values, value);
  for (auto& new_field : values) {
    bool found = false;
    for (auto& current_field : current_values) {
      if (current_field.first == new_field.first) {
        found = true;
        current_field.second = new_field.second;
        break;
      }
    }
    if (found == false) {
      break;
    }
  }

  value.clear();
  SerializeRow(current_values, value);
  s = db_->Put(rocksdb::WriteOptions(), key, value);
  if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  return Status::kOK;
}

Status RocksDB::Delete(const std::string& table, const std::string& key)
{
  rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), key);
  if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  return Status::kOK;
}

void RocksDB::printStats()
{
  std::string stats;
  db_->GetProperty("rocksdb.stats", &stats);
  cerr << stats << endl;
  cerr << options.statistics->ToString() << endl;
}

RocksDB::~RocksDB()
{
  printStats();
  delete db_;
}
} //ycsbc
