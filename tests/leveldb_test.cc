#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <cstdio>

#include "utils.h"
#include "basic_db.h"
#include "core/db.h"
#include "db/db_factory.h"
#include "db/leveldb_db.h"



TEST(LevelDBTest, TestOperationInsert){
  ASSERT_EQ(0, mkdir("tmpdb", 0777));
  auto dbptr = std::make_shared<ycsbc::LevelDB>("tmpdb");
  const std::string table = "leveldb_table";
  const std::string key = "leveldb_key1";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1","value1"));
  write_field.push_back(std::make_pair("field2","value2"));
  write_field.push_back(std::make_pair("field3","value3"));

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
}

TEST(LevelDBTest, TestOperationRead){
  auto dbptr = std::make_shared<ycsbc::LevelDB>("tmpdb");
  const std::string table = "leveldb_table";
  const std::string key = "leveldb_key2";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1","value1"));
  write_field.push_back(std::make_pair("field2","value2"));
  write_field.push_back(std::make_pair("field3","value3"));

  std::vector<std::string> read_field = {"field1", "field2", "field3"};
  std::vector<ycsbc::DB::KVPair> result;

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Read(table, key, &read_field, result));
}

TEST(LevelDBTest, TestOperationScan){
  auto dbptr = std::make_shared<ycsbc::LevelDB>("tmpdb");
  const std::string table = "leveldb_table";
  const std::string key = "leveldb_key3";
  std::vector<ycsbc::DB::KVPair> write_field;
  write_field.push_back(std::make_pair("field1","value1"));
  write_field.push_back(std::make_pair("field2","value2"));
  write_field.push_back(std::make_pair("field3","value3"));

  std::vector<std::string> read_field = {"field1", "field2", "field3"};
  std::vector<std::vector<ycsbc::DB::KVPair>> result;

  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Insert(table, key, write_field));
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Scan(table, key, 3, &read_field, result));
}

TEST(LevelDBTest, TestOperationDelete){
  auto dbptr = std::make_shared<ycsbc::LevelDB>("tmpdb");
  const std::string table = "leveldb_table";
  const std::string key = "leveldb_key1";

  std::vector<std::string> read_field = {"field1", "field2", "field3"};
  std::vector<ycsbc::DB::KVPair> result;
  ASSERT_EQ(ycsbc::Status::kOK, dbptr->Delete(table, key));
  ASSERT_EQ(ycsbc::Status::kErrorNoData, dbptr->Read(table, key, &read_field, result));
  ASSERT_EQ(0, system("rm -r tmpdb"));
}





int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
