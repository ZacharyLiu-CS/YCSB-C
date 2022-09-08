//
//  fastfair_test.cc
//  PROJECT fastfair_test
//
//  Created by zhenliu on 06/09/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "concurrent_pmdk/src/btree.h"
#include "core/config_reader.h"
#include "core/db.h"
#include "pmem_engine.h"
#include "pmem_log.h"
#include "gtest/gtest.h"
#include <string>

const std::string db_path = "/mnt/pmem0/tmp-fastfair";
const std::string clean_cmd = "rm -r /mnt/pmem0/tmp-fastfair";
const std::string mkdir_cmd = "mkdir -p /mnt/pmem0/tmp-fastfair";
using namespace fastfair;
#define LOG(msg)                                                               \
  do {                                                                         \
    std::cout << msg << std::endl;                                             \
  } while (0)

TEST(UTreeTest, TestBasicOperation) {
  ASSERT_EQ(0, system(mkdir_cmd.c_str()));
  TOID(btree) bt_ = TOID_NULL(btree);
  PMEMobjpool *pop_ = nullptr;

  char full_path[100];
  sprintf(full_path, "%s/btree", db_path.c_str());
  if (!file_exists(full_path)) {
    remove(full_path);
  }
  pop_ = pmemobj_create(full_path, "btree", 1ULL << 30, 0666);
  bt_ = POBJ_ROOT(pop_, btree);
  D_RW(bt_)->constructor(pop_);

  NKV::PmemEngine *engine_ptr = nullptr;
  NKV::PmemEngineConfig engine_config;
  engine_config.chunk_size = 128ULL << 20;
  engine_config.engine_capacity = 1ULL << 30;
  std::string shard_path = db_path + "/pmemlog";
  strcpy(engine_config.engine_path, shard_path.c_str());
  NKV::PmemEngine::open(engine_config, &engine_ptr);

  int insert_ops = 1000;
  std::cout << "start to insert " << std::endl;
  // insert value
  for (int i = 0; i < insert_ops; i++) {
    std::string value = std::to_string(i);
    NKV::PmemAddress addr;
    engine_ptr->append(addr, value.data(), value.size());
    D_RW(bt_)->btree_insert(i, (char *)addr);
    std::cout << "Key: " << i << "Value ptr: " << addr << " Value: " << value
              << std::endl;
  }
  std::cout << "start to read " << std::endl;
  // read value
  for (int i = 0; i < insert_ops; i++) {
    std::string expect_value = std::to_string(i);
    char *addr = D_RW(bt_)->btree_search(i);
    std::string value;
    engine_ptr->read((NKV::PmemAddress)addr, value);
    std::cout << "Key: " << i << "Value ptr: " << (NKV::PmemAddress)addr
              << " Value: " << value << std::endl;
  }
  std::cout << "start to scan " << std::endl;
  // scan value
  int scan_length = 100;
  for (int start = 0; start < insert_ops; start += scan_length) {
    uint64_t valueptr_list[scan_length];
    D_RW(bt_)->btree_search_range(start-1, INT32_MAX, valueptr_list, scan_length);
    for (auto i = 0; i < scan_length; i++) {
      std::string value;
      if (valueptr_list[i] == (unsigned long)NULL) {
        continue;
      }
      value.clear();
      engine_ptr->read((NKV::PmemAddress)valueptr_list[i], value);
      std::cout << "Key: " << start + i << "Value ptr: " << valueptr_list[i]
                << " Value: " << value << std::endl;
    }
  }

  pmemobj_close(pop_);
  delete engine_ptr;
}

TEST(ListDBTest, TestOperationDelete) {
  ASSERT_EQ(0, system(clean_cmd.c_str()));
}
//
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
