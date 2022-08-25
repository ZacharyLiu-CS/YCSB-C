//
//  utree_test.cc
//  PROJECT utree_test
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
#include <iostream>
#include <memory>

#include "utree.h"

const std::string db_path = "/mnt/pmem0/tmp-utree";
const std::string clean_cmd = "rm -r /mnt/pmem0/tmp-utree";
const std::string mkdir_cmd = "mkdir -p /mnt/pmem0/tmp-utree";


#define LOG(msg)                   \
  do {                             \
    std::cout << msg << std::endl; \
  } while (0)

TEST(UTreeTest, TestBasicOperation)
{
  ASSERT_EQ(0, system(mkdir_cmd.c_str()));

  btree *bt = new btree(db_path, 16ULL << 30);
  entry_key_t key = 1;
  std::string value =  "11";
  bt->insert(key, const_cast<char*>(value.c_str()));
  char * v = bt->search(key);
  ASSERT_EQ((uint64_t)v, (uint64_t)value.c_str());
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
