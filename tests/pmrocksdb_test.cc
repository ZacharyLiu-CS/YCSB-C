//
//  pmrocksdb_test.cc
//  PROJECT pmrocksdb_test
//
//  Created by zhenliu on 30/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

const std::string pmem_rocksdb_path = "/mnt/pmem0/tmp-pmemrocksdb";
TEST(RocksDBTest, TestBasciOperation) {

  rocksdb::DB *db_;
  rocksdb::Options options;
  options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());
  options.create_if_missing = true;

  // configure for pmem kv sepeartion
  options.wal_dir = pmem_rocksdb_path + "/wal";
  options.dcpmm_kvs_enable = true;
  options.dcpmm_kvs_mmapped_file_fullpath = pmem_rocksdb_path + "/kvs";
  options.dcpmm_kvs_mmapped_file_size = 2ULL << 30;
  options.dcpmm_kvs_value_thres = 64; // minimal size to do kv sep
  options.dcpmm_compress_value = false;

  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.allow_dcpmm_writes = true;
  rocksdb::Status s = rocksdb::DB::Open(options, pmem_rocksdb_path, &db_);
  ASSERT_EQ(s.ok(), true);

  // test insert
  std::string key1 = "key1";
  std::string value1 = "value1";
  s = db_->Put(rocksdb::WriteOptions(), key1, value1);
  ASSERT_EQ(s.ok(), true);

  // test read
  std::string read_value;
  s = db_->Get(rocksdb::ReadOptions(), key1, &read_value);
  ASSERT_EQ(s.ok(), true);
  ASSERT_EQ(value1, read_value);

  // test scan
  for (auto i = 2; i <= 9; i++) {
    std::string tmp_key = "key" + std::to_string(i);
    std::string tmp_value = "value" + std::to_string(i);
    db_->Put(rocksdb::WriteOptions(), tmp_key, tmp_value);
  }

  // now we scan the data
  auto iter = db_->NewIterator(rocksdb::ReadOptions());
  iter->Seek(key1);
  for (auto i = 1; i <= 9; i++) {
    std::string tmp_value = "value" + std::to_string(i);
    ASSERT_EQ(tmp_value, iter->value().ToString());
    iter->Next();
  }

  // close the db

  db_->Close();
}

TEST(RocksDBTest, TestOperationDelete) {
  ASSERT_EQ(0, system(std::string("rm -r ").append(pmem_rocksdb_path).c_str()));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
