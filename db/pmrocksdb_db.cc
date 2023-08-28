//
//  rocksdb_db.cc
//  YCSB-C
//
//  Created by zhenliu on 18/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pmrocksdb_db.h"

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <vector>

#include "db.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

using std::cerr;
using std::cout;
using std::endl;

#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {

PMRocksDB::PMRocksDB(const char *dbfilename) : no_found_(0) {
  ConfigReader config_reader = ConfigReader();
  db_config *dc =
      static_cast<db_config *>(config_reader.get_config("pmrocksdb").get());
  // create database if not exists
  options.create_if_missing = true;
  options.use_direct_reads = dc->enable_direct_io_;
  options.use_direct_io_for_flush_and_compaction = dc->enable_direct_io_;
  options.disable_auto_compactions = !dc->enable_compaction_;
  options.max_background_compactions = dc->thread_compaction_;
  options.max_background_jobs = dc->thread_compaction_ + 4;
  options.write_buffer_size = dc->memtable_size_;
  options.target_file_size_base = dc->sst_file_size_;
  options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());

  // setup the pmem configuration
  std::string pmem_rocksdb_path = dbfilename;
  // configure for pmem kv sepeartion
  options.wal_dir = pmem_rocksdb_path + "/wal";
  options.dcpmm_kvs_enable = true;
  options.dcpmm_kvs_mmapped_file_fullpath = pmem_rocksdb_path + "/kvs";
  options.dcpmm_kvs_mmapped_file_size = dc->db_size_;
  options.dcpmm_kvs_value_thres = 64; // minimal size to do kv sep
  options.dcpmm_compress_value = false;
  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.allow_dcpmm_writes = true;

  rocksdb::BlockBasedTableOptions block_options;
  block_options.filter_policy.reset(
      rocksdb::NewBloomFilterPolicy(dc->bloom_bits_));
  block_options.block_cache = rocksdb::NewLRUCache(dc->block_cache_size_);

  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(block_options));
  options.statistics = rocksdb::CreateDBStatistics();

  rocksdb::Status s = rocksdb::DB::Open(options, dbfilename, &db_);
  if (!s.ok()) {
    LOGOUT("init rocksdb failed!");
  }
}

Status PMRocksDB::Read(const std::string &table, const std::string &key,
                       const std::vector<std::string> *fields,
                       std::vector<KVPair> &result) {
  // encoding by column
  if (encoding_by_row_ == false) {
    // read all field
    if (fields == nullptr) {
      for (auto i = 0; i < field_count_; i++) {
        std::string field_key = key;
        field_key.append("field").append(std::to_string(i));
        std::string field_value;
        rocksdb::Status s =
            db_->Get(rocksdb::ReadOptions(), field_key, &field_value);
        result.push_back({field_key, field_value});
      }
      return Status::kOK;
    }
    // read only some fields
    for (auto &field_name : *fields) {
      std::string field_value;
      std::string field_key = key;
      field_key.append(field_name);
      rocksdb::Status s =
          db_->Get(rocksdb::ReadOptions(), field_key, &field_value);
      result.push_back({field_key, field_value});
    }
    return Status::kOK;
  }
  // encoding by row;
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

Status PMRocksDB::Scan(const std::string &table, const std::string &key,
                       int len, const std::vector<std::string> *fields,
                       std::vector<std::vector<KVPair>> &result) {
  // encoding by column
  rocksdb::Iterator *iter = db_->NewIterator(rocksdb::ReadOptions());
  iter->Seek(key);

  if (encoding_by_row_ == false) {
    // read all fields
    if (fields == nullptr) {
      for (int i = 0; iter->Valid() && i < len; i++) {
        result.push_back(std::vector<KVPair>());
        std::vector<KVPair> &values = result.back();
        for (auto i = 0; i < field_count_; i++) {
          std::string field_value = iter->value().ToString();
          std::string field_key =
              std::string("field").append(std::to_string(i));
          values.push_back({field_key, field_value});
          iter->Next();
        }
      }
      return Status::kOK;
    }
    // read only some fields
    for (int i = 0; iter->Valid() && i < len; i++) {
      result.push_back(std::vector<KVPair>());
      std::vector<KVPair> &values = result.back();
      for (auto i = 0; i < field_count_; i++) {
        std::string field_value = iter->value().ToString();
        std::string field_key = std::string("field").append(std::to_string(i));
        if (std::find(fields->begin(), fields->end(), field_key) !=
            fields->end()) {
          values.push_back({field_key, field_value});
        }
        iter->Next();
      }
    }
    return Status::kOK;
  }
  // encoding by row
  for (int i = 0; iter->Valid() && i < len; i++) {
    std::string value = iter->value().ToString();
    result.push_back(std::vector<KVPair>());
    std::vector<KVPair> &values = result.back();
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

Status PMRocksDB::Insert(const std::string &table, const std::string &key,
                         std::vector<KVPair> &values) {

    // encoding by column
  if (encoding_by_row_ == false) {
    rocksdb::Status s;
    for (auto &[field_name, field_value] : values) {
      std::string field_key = key + field_name;
      s = db_->Put(rocksdb::WriteOptions(), field_key, field_value);
    }
    if (!s.ok()) {
      LOGOUT(s.ToString());
    }
    return Status::kOK;
  }
  // encoding by row
  std::string value;
  SerializeRow(values, value);

  rocksdb::Status s;
  s = db_->Put(rocksdb::WriteOptions(), key, value);
  if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  return Status::kOK;
}

Status PMRocksDB::Update(const std::string &table, const std::string &key,
                         std::vector<KVPair> &values) {
  // encoding by column
  if (encoding_by_row_ == false) {
    rocksdb::Status s;
    for (auto &[field_name, field_value] : values) {
      std::string field_key = key + field_name;
      s = db_->Put(rocksdb::WriteOptions(), field_key, field_value);
    }
    if (!s.ok()) {
      LOGOUT(s.ToString());
    }
    return Status::kOK;
  }

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
  for (auto &new_field : values) {
    bool found = false;
    for (auto &current_field : current_values) {
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

Status PMRocksDB::Delete(const std::string &table, const std::string &key) {
  // if encoding by column
  if (encoding_by_row_ == false) {
    rocksdb::Status s;
    for (auto i = 0; i < field_count_; i++) {
      std::string field_key = key;
      field_key.append("field").append(std::to_string(i));
      s = db_->Delete(rocksdb::WriteOptions(), field_key);
    }
    if (!s.ok()) {
      LOGOUT(s.ToString());
    }
    return Status::kOK;
  }
  rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), key);
  if (!s.ok()) {
    LOGOUT(s.ToString());
  }
  return Status::kOK;
}

void PMRocksDB::printStats() {
  // std::string stats;
  // db_->GetProperty("rocksdb.stats", &stats);
  // cerr << stats << endl;
  // cerr << options.statistics->ToString() << endl;
  std::cout << "print pmrocksdb statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
}

PMRocksDB::~PMRocksDB() {
  printStats();
  delete db_;
}
} // namespace ycsbc
