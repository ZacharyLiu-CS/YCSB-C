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

#include "pmem_engine.h"
#include "pmem_log.h"
#include "utree.h"

const std::string db_path = "/mnt/pmem0/tmp-utree";
const std::string clean_cmd = "rm -r /mnt/pmem0/tmp-utree";
const std::string mkdir_cmd = "mkdir -p /mnt/pmem0/tmp-utree";

#define LOG(msg)                                                               \
  do {                                                                         \
    std::cout << msg << std::endl;                                             \
  } while (0)

TEST(UTreeTest, TestBasicOperation) {
  ASSERT_EQ(0, system(mkdir_cmd.c_str()));

  NKV::PmemEngine *engine_ptr = nullptr;
  NKV::PmemEngineConfig engine_config;
  engine_config.chunk_size = 128ULL << 20;
  engine_config.engine_capacity = 1ULL << 30;
  std::string shard_path = db_path + "/pmemlog";
  strcpy(engine_config.engine_path, shard_path.c_str());
  NKV::PmemEngine::open(engine_config, &engine_ptr);
  
  btree *bt = new btree(db_path, 16ULL << 30);
  entry_key_t key = 1;
  std::string value = "11";
  NKV::PmemAddress addr;
  engine_ptr->append(addr, value.data(), value.size());

  bt->insert(key, (char*)addr);
  char *v = bt->search(key);
  ASSERT_EQ((uint64_t)v, addr);

  std::string read_value;
  engine_ptr->read(addr, read_value);
  ASSERT_EQ(value, read_value);
}

TEST(ListDBTest, TestOperationDelete) {
  ASSERT_EQ(0, system(clean_cmd.c_str()));
}
//
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
