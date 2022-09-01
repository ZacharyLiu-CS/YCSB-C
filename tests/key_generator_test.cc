//
//  key_generator_test.cc
//  PROJECT key_generator_test
//
//  Created by zhenliu on 01/09/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "core/core_workload.h"
#include "gtest/gtest.h"

#include <cstdint>
#include <iostream>
#include <string>

class TestGenerator : public ycsbc::CoreWorkload {
public:
  std::string GenerateKey(uint64_t key) { return this->BuildKeyName(key); }
};

uint64_t getinside(const std::string &key) {
  return std::stoull(key.substr(4, -1));
}
TEST(KeyGeneratorTest, GenerateAndReadKeyTest) {
  TestGenerator tg;
  uint64_t key_num = 1234567ULL;
  std::string test_key = tg.GenerateKey(key_num);
  std::cout << test_key << std::endl;
  std::cout << getinside(test_key) << std::endl;
  key_num = UINT64_MAX;
  test_key = tg.GenerateKey(key_num);
  std::cout << test_key << std::endl;
  std::cout << getinside(test_key) << std::endl;

  // auto read_key = ycsbc::CoreWorkload::GetIntFromKey(test_key.c_str());
  // ASSERT_EQ(read_key, key_num);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
