//
//  neopmkv_test.cc
//  PROJECT neopmkv_test
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
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

#include "neopmkv.h"



const std::string mkdir_cmd = "mkdir -p /mnt/pmem0/tmp-neopmkv";
const std::string clean_cmd = "rm -r /mnt/pmem0/tmp-neopmkv";

#define LOG(msg)                   \
  do {                             \
    std::cout << msg << std::endl; \
  } while (0)

TEST(NEOPMKVTest, TestBasicOperation)
{

  ASSERT_EQ(0, system(mkdir_cmd.c_str()));

  NKV::NeoPMKV * neopmkv = new NKV::NeoPMKV();
  NKV::EntryKey key1 = 1;
  std::string value1 = "11";
  NKV::EntryKey key2 = 2;
  std::string value2 = "22";
  neopmkv->put(key1, value1);
  neopmkv->put(key2, value2);

  std::string read_value;
  neopmkv->get(key1, read_value);
  ASSERT_EQ(read_value, value1);
  neopmkv->get(key2, read_value);
  ASSERT_EQ(read_value, value2);

  delete neopmkv;

}
TEST(NEOPMKVTest, TestOperationDelete)
{
  ASSERT_EQ(0, system(clean_cmd.c_str()));
}
//
int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
