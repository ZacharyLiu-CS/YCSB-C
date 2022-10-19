//
//  zipfian_generator_test.cc
//  PROJECT zipfian_generator_test
//
//  Created by zhenliu on 15/10/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "core/timer.h"
#include "core/utils.h"
#include "core/zipfian_generator.h"
#include "core/scrambled_zipfian_generator.h"
#include "histogram.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <bits/stdint-uintn.h>
#include <chrono>
#include <future>
#include <iostream>

class ZipfianGeneratorTest {
public:
  ycsbc::ZipfianGenerator generator_;
  utils::Timer<utils::t_microseconds> timer_;
  double test_count_;
  std::mutex print_mutex_;
  ZipfianGeneratorTest(double test_cout)
      : test_count_(test_cout), generator_(1, test_cout + 1) {}

  void RunSingleThreadTest(double count = 0, bool print_value = false) {
    if (count == 0) {
      count = test_count_;
    }
    timer_.Start();
    for (auto i = 0; i < count; i++) {
      uint64_t a = generator_.Next();
      if(print_value == true){
        std::cout << i << " : " << a << std::endl;
      }
    }
    auto duration = timer_.End();
    print_mutex_.lock();
    std::cout << "Zipfian Test Count: "<< count << "\nDuration time: "
              << duration / utils::RecordUnit::h_microseconds << "s"
              << std::endl;
    std::cout << "Average time: " << duration / count << "μs"
              << std::endl;
    print_mutex_.unlock();
  }

  void RunMultiThreadTest(uint32_t thread_num, bool print_value = false) {
    std::vector<std::future<void>> thread_list;
    for (auto thread_index = 0; thread_index < thread_num; thread_index++) {
      thread_list.emplace_back(std::async(
          std::launch::async, &ZipfianGeneratorTest::RunSingleThreadTest, this,
          test_count_ / thread_num, print_value));
    }
    for(auto &i : thread_list){
      i.get();
    }
  }
};

class ScrambledZipfianGeneratorTest {
public:
  ycsbc::ScrambledZipfianGenerator *generator_;
  utils::Timer<utils::t_microseconds> timer_;
  double test_count_;
  std::mutex print_mutex_;
  ScrambledZipfianGeneratorTest(double test_cout)
      : test_count_(test_cout){
        generator_ = new ycsbc::ScrambledZipfianGenerator(1, test_cout + 1);
      }
      ~ScrambledZipfianGeneratorTest(){delete generator_;}

  void RunSingleThreadTest(double count = 0, bool print_value = false) {
    if (count == 0) {
      count = test_count_;
    }
    timer_.Start();
    for (auto i = 0; i < count; i++) {
      uint64_t a = generator_->Next();
      if(print_value == true){
        std::cout << i << " : " << a << std::endl;
      }
    }
    auto duration = timer_.End();
    print_mutex_.lock();
    std::cout << "Scrambled zipfian Test Count: "<< count << "\nDuration time: "
              << duration / utils::RecordUnit::h_microseconds << "s"
              << std::endl;
    std::cout << "Average time: " << duration / count << "μs"
              << std::endl;
    print_mutex_.unlock();
  }

  void RunMultiThreadTest(uint32_t thread_num, bool print_value = false) {
    std::vector<std::future<void>> thread_list;
    for (auto thread_index = 0; thread_index < thread_num; thread_index++) {
      thread_list.emplace_back(std::async(
          std::launch::async, &ScrambledZipfianGeneratorTest::RunSingleThreadTest, this,
          test_count_ / thread_num, print_value));
    }
    for(auto &i : thread_list){
      i.get();
    }
  }
};
TEST(ZipfianGeneratorTest, TestPerformance) {
  ZipfianGeneratorTest zipfian_test(10 << 20);
  zipfian_test.RunSingleThreadTest();

  zipfian_test.RunMultiThreadTest(2);

  zipfian_test.RunMultiThreadTest(4);

  zipfian_test.RunMultiThreadTest(8);
  
  zipfian_test.RunMultiThreadTest(16);
}
TEST(ScrambledZipfianGeneratorTestTest, TestPerformance) {
  ScrambledZipfianGeneratorTest scrambled_zipfian_test(20 << 20);
  scrambled_zipfian_test.RunSingleThreadTest();

  scrambled_zipfian_test.RunMultiThreadTest(2);

  scrambled_zipfian_test.RunMultiThreadTest(4);

  scrambled_zipfian_test.RunMultiThreadTest(8);
  
  scrambled_zipfian_test.RunMultiThreadTest(16);
}


int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
