//
//  listdb_test.cc
//  project listdb_test
//
//  created by zhenliu on 24/08/2022.
//  copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "listdb/db_client.h"

#include "listdb/listdb.h"

const uint64_t POOL_SIZE = 64UL * 1024UL * 1024UL;
const std::string db_path = "/mnt/pmem0/tmp-listdb";
const std::string clean_cmd = "rm -r /mnt/pmem0/tmp-listdb";
const std::string mkdir_cmd = "mkdir -p /mnt/pmem0/tmp-listdb";



#define LOG(msg)                   \
  do {                             \
    std::cout << msg << std::endl; \
  } while (0)
inline std::string convert_valueptr_to_value(uint64_t& value_ptr) {
  // std::cerr << "value:ptr" << value_ptr << std::endl;
  size_t value_len = *((size_t *)value_ptr);
  // std::cerr << "value:len" << value_len << std::endl;
  value_ptr += sizeof(size_t);
  std::string value;
  value.assign((char*)value_ptr, value_len);
  // std::cerr << "value:content" << value << std::endl;
  return value;
}
TEST(ListDBTest, TestBasicOperation)
{

  ASSERT_EQ(0, system(mkdir_cmd.c_str()));

  ListDB* db = new ListDB();
  db->Init(db_path, POOL_SIZE);
  DBClient* client = new DBClient(db, 0, 0);
  std::string key1 = "key1";
  std::string value1 = "value1";
  std::string key2 = "key2";
  std::string value2 = "value2";

  client->PutStringKV(key1, value1);
  client->PutStringKV(key2, value2);

  //read value1
  uint64_t value_ptr;
  ASSERT_EQ(client->GetStringKV(key1, &value_ptr), true);
  std::string value = convert_valueptr_to_value(value_ptr);
  ASSERT_EQ(value, value1);
  
  //read value2
  ASSERT_EQ(client->GetStringKV(key2, &value_ptr), true);
  value = convert_valueptr_to_value(value_ptr);
  ASSERT_EQ(value, value2);
  delete client;
  delete db;


}

TEST(ListDBTest, TestOperationDelete)
{
  ASSERT_EQ(0, system(clean_cmd.c_str()));
}
//
int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
