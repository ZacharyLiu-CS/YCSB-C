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


const uint64_t POOL_SIZE = 64UL * 1024UL * 1024UL;
const std::string db_path = "/mnt/pmem0/tmp-listdb";
const std::string clean_cmd = "rm -r /mnt/pmem0/tmp-listdb";
const std::string mkdir_cmd = "mkdir -p /mnt/pmem0/tmp-listdb";

#define LOG(msg)                   \
  do {                             \
    std::cout << msg << std::endl; \
  } while (0)

TEST(ListDBTest, TestBasicOperation)
{

  ASSERT_EQ(0, system(mkdir_cmd.c_str()));



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
