#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <libpmemkv.hpp>
#include <memory>
#include <string>
#include <utility>

#include "basic_db.h"
#include "core/db.h"
#include "db/db_factory.h"
#include "db/pmemkv_db.h"
#include "utils.h"

using namespace pmem::kv;

const uint64_t SIZE = 1024UL * 1024UL * 1024UL;

TEST(PmemKVTest, TestBasicOperation)
{
  ASSERT_EQ(0, mkdir("/mnt/pmem0/tmp-pmemkv", 0777));
  config cfg;
  status s = cfg.put_path("/mnt/pmem0/tmp-pmemkv/cmap");
  ASSERT_EQ(true, s == status::OK);

  s = cfg.put_size(SIZE);
  ASSERT_EQ(true, s == status::OK);

  s = cfg.put_create_if_missing(true);
  ASSERT_EQ(true, s == status::OK);

  db kv;
  s = kv.open("cmap", std::move(cfg));
  ASSERT_EQ(true, s == status::OK);

  const std::string key = "pmemkv_key1";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1", "value1"));
  write_field.push_back(std::make_pair("field2", "value2"));
  write_field.push_back(std::make_pair("field3", "value3"));

  std::string value;
  ycsbc::DeserializeRow(write_field, value);

  s = kv.put(key, value);
  ASSERT_EQ(true, s == status::OK);

  size_t cnt;
  s = kv.count_all(cnt);
  ASSERT_EQ(true, s == status::OK);
  ASSERT_EQ(true, cnt == 1);


  //read key back
  std::string read_value;
  s = kv.get(key, &read_value);
  ASSERT_EQ(true, s == status::OK);
  ASSERT_EQ(value, read_value);

  //remove the key
  s = kv.remove(key);
  ASSERT_EQ(true, s == status::OK);
  s = kv.exists(key);
  ASSERT_EQ(true, s == status::NOT_FOUND);
}

TEST(PmemKVTest, TestOperationInsert){
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key1";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1","value1"));
  write_field.push_back(std::make_pair("field2","value2"));
  write_field.push_back(std::make_pair("field3","value3"));

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
}

TEST(PmemKVTest, TestOperationRead){
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key2";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1","value1"));
  write_field.push_back(std::make_pair("field2","value2"));
  write_field.push_back(std::make_pair("field3","value3"));

  std::vector<std::string> read_field = {"field1", "field2", "field3"};
  std::vector<ycsbc::DB::KVPair> result;

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Read(table, key, &read_field, result));
}

TEST(PmemKVTest, TestOperationScan){
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key3";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1","value1"));
  write_field.push_back(std::make_pair("field2","value2"));
  write_field.push_back(std::make_pair("field3","value3"));

  std::vector<std::string> read_field = {"field1", "field2", "field3"};
  std::vector<std::vector<ycsbc::DB::KVPair>> result;

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Scan(table, key, 3, &read_field, result));
}

TEST(PmemKVTest, TestOperationDelete){
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key1";

  std::vector<std::string> read_field = {"field1", "field2", "field3"};
  std::vector<ycsbc::DB::KVPair> result;
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Delete(table, key));
  ASSERT_EQ(ycsbc::Status::kErrorNoData, dbptr->Read(table, key, &read_field, result));
  ASSERT_EQ(0, system("rm -r /mnt/pmem0/tmp-pmemkv"));
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
