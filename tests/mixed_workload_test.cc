//
//  mixed_workload_test.cc
//  PROJECT mixed_workload_test
//
//  Created by zhenliu on 31/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "gtest/gtest.h"
#include <algorithm>
#include <bits/stdint-uintn.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <ostream>

#include "core/mixed_workload.h"
#include "core/uniform_generator.h"
#include "properties.h"

TEST(MixedWorkloadTest, MixedInitializationTest) {
  std::string filename = "../workloads/mixedworkload_template.spec";
  utils::Properties props;
  std::ifstream input(filename);
  props.Load(input);
  props.SetProperty("propsfile", filename);
  input.close();
  ycsbc::MixedWorkload mixed_workload;
  int thread_count = 12;
  mixed_workload.Init(props, thread_count);
  // std::cout << mixed_workload.ToString() << std::endl;
  std::string expected_output;
  expected_output.append("Workload Mixed : workloada.spec;\t Thread Count : 4\n");
  expected_output.append("Workload Mixed : workloadb.spec;\t Thread Count : 4\n");
  expected_output.append("Workload Mixed : workloadc.spec;\t Thread Count : 4\n");

  ASSERT_EQ(mixed_workload.ToString(), expected_output);
}

TEST(MixedWorkloadTest, SingleInitializationTest) {
  std::string filename = "../workloads/workloada.spec";
  utils::Properties props;
  std::ifstream input(filename);
  props.Load(input);
  props.SetProperty("propsfile", filename);
  input.close();
  ycsbc::MixedWorkload mixed_workload;
  int thread_count = 12;
  mixed_workload.Init(props, thread_count);
  std::string expected_output;
  expected_output.append("Workload Single;\t Thread Count : 12\n");

  // std::cout << mixed_workload.ToString() << std::endl;
  ASSERT_EQ(mixed_workload.ToString(), expected_output);
}
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
