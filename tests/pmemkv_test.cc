//
//  pmemkv_test.cc
//  PROJECT pmemkv_test
//
//  Created by zhenliu on 24/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//



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

#define LOG(msg) std::cout << msg << std::endl

std::string get_key(pmem::kv::db::read_iterator& it)
{
  pmem::kv::result<string_view> key_result = it.key();
  return key_result.get_value().data();
}

std::string get_value(pmem::kv::db::read_iterator& it)
{
  pmem::kv::result<string_view> val_result = it.read_range();
  return val_result.get_value().data();
}

TEST(PmemKVTest, TestBasicOperation)
{
  ASSERT_EQ(0, system("mkdir -p /mnt/pmem0/tmp-pmemkv"));
  config cfg;
  status s = cfg.put_path("/mnt/pmem0/tmp-pmemkv/cmap");
  ASSERT_EQ(true, s == status::OK);

  s = cfg.put_size(SIZE);
  ASSERT_EQ(true, s == status::OK);

  s = cfg.put_create_if_missing(true);
  ASSERT_EQ(true, s == status::OK);

  auto kv = std::unique_ptr<db>(new db());
  s = kv->open("radix", std::move(cfg));
  ASSERT_EQ(true, s == status::OK);

  const std::string key = "pmemkv_key1";

  std::string value;
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1", "value1"));
  write_field.push_back(std::make_pair("field2", "value2"));
  write_field.push_back(std::make_pair("field3", "value3"));
  ycsbc::SerializeRow(write_field,value);

  std::vector<ycsbc::DB::KVPair> deser_vec;
  ycsbc::DeserializeRow(deser_vec, value);

  // for( auto &i : deser_vec ){
  //   LOG("Key:" + i.first);
  //   LOG("Value:" + i.second);
  // }

  //insert the kv
  s = kv->put(key, value);
  ASSERT_EQ(true, s == status::OK);

  size_t cnt;
  s = kv->count_all(cnt);
  ASSERT_EQ(true, s == status::OK);
  ASSERT_EQ(true, cnt == 1);

  const std::string key2 = "pmemkv_key2";
  const std::string key3 = "pmemkv_key3";

  s = kv->put(key2, value);
  ASSERT_EQ(true, s == status::OK);

  s = kv->put(key3, value);
  ASSERT_EQ(true, s == status::OK);

  s = kv->count_all(cnt);
  ASSERT_EQ(true, s == status::OK);
  ASSERT_EQ(true, cnt == 3);

  //read the key
  s = kv->get(key, &value);
  // LOG("key: " + key + " value: " + value);

  //scan the key pmem_key1 -> pmem_key3
  auto res_it = kv->new_read_iterator();
  ASSERT_EQ(true, res_it.is_ok());
  auto& it = res_it.get_value();

  // LOG("Iterate from first to last element");
  s = it.seek_to_first();
  do {
    /* read a key */
    auto key = get_key(it);
    // LOG("key: " + key);
    /* read a value */
    auto value = get_value(it);
    // LOG("value: " + value);

  } while (it.next() == status::OK);


  //remove the key
  s = kv->remove(key);
  ASSERT_EQ(true, s == status::OK);
  s = kv->exists(key);
  ASSERT_EQ(true, s == status::NOT_FOUND);

  ASSERT_EQ(0, system("rm -r /mnt/pmem0/tmp-pmemkv"));
}


TEST(PmemKVTest, TestOperationInsert)
{
  ASSERT_EQ(0, system("mkdir -p /mnt/pmem0/tmp-pmemkv"));
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key1";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1", "value1"));
  write_field.push_back(std::make_pair("field2", "value2"));
  write_field.push_back(std::make_pair("field3", "value3"));

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
}

TEST(PmemKVTest, TestOperationRead)
{
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key2";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1", "value1"));
  write_field.push_back(std::make_pair("field2", "value2"));
  write_field.push_back(std::make_pair("field3", "value3"));

  std::vector<std::string> read_field = { "field1", "field2", "field3" };
  std::vector<ycsbc::DB::KVPair> result;

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Read(table, key, &read_field, result));
}

TEST(PmemKVTest, TestOperationScan)
{
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key3";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1", "value1"));
  write_field.push_back(std::make_pair("field2", "value2"));
  write_field.push_back(std::make_pair("field3", "value3"));

  std::vector<std::string> read_field = { "field1", "field2", "field3" };
  std::vector<std::vector<ycsbc::DB::KVPair>> result;

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Scan(table, "pmemkv_key1", 3, &read_field, result));
}

TEST(PmemKVTest, TestOperationDelete)
{
  auto dbptr = std::make_shared<ycsbc::PmemKV>("/mnt/pmem0/tmp-pmemkv");
  const std::string table = "pmemkv_table";
  const std::string key = "pmemkv_key1";

  std::vector<std::string> read_field = { "field1", "field2", "field3" };
  std::vector<ycsbc::DB::KVPair> result;
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Delete(table, key));
  ASSERT_EQ(ycsbc::Status::kErrorNoData, dbptr->Read(table, key, &read_field, result));
  ASSERT_EQ(0, system("rm -r /mnt/pmem0/tmp-pmemkv"));
}
//
int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
