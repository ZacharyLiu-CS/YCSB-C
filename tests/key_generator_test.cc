//
//  key_generator_test.cc
//  PROJECT key_generator_test
//
//  Created by zhenliu on 01/09/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "gtest/gtest.h"
#include "core/core_workload.h"

#include <string>
#include <iostream>
#include <cstdint>

class TestGenerator : public ycsbc::CoreWorkload{
    public :
    std::string GenerateKey(uint64_t key){
        return this->BuildKeyName(key);
    }
};


TEST(KeyGeneratorTest, GenerateAndReadKeyTest){
    TestGenerator tg;
    uint64_t key_num = 1234567ULL;
    std::string test_key = tg.GenerateKey(key_num);

    auto read_key = ycsbc::CoreWorkload::GetIntFromKey(test_key.c_str());
    ASSERT_EQ(read_key, key_num);
}


int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

