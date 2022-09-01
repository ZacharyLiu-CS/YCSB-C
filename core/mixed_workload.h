//
//  mixed_workload.h
//  PROJECT mixed_workload
//
//  Created by zhenliu on 30/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include "core_workload.h"
#include "properties.h"
#include <fstream>
#include <iostream>
#include <string>
#include <utility>

namespace ycsbc {

enum WorkloadType : uint32_t { Single = 0, Mixed = 1 };

enum SubWorkloadType : uint32_t {
  WORKLOAD_A = 0,
  WORKLOAD_B,
  WORKLOAD_C,
  WORKLOAD_D,
  WORKLOAD_E,
  WORKLOAD_F,
  MAX_TYPE
};

class MixedWorkload {
public:
  // workload tpye
  static const std::string WORKLOAD_TYPE_PROPERTY;
  static const std::string WORKLOAD_TYPE_DEFAULT;

  // sub workload unit
  static const std::vector<std::string> SUBWORKLOAD_PROPERTY_LIST;
  static const std::string SUBWORKLOAD_UNIT_DEFAULT;

  static const std::vector<std::string> SUBWORKLOAD_RECORDCOUNT_PROPERTY_LIST;
  static const std::string SUBWORKLOAD_RECORDCOUNT_DEFAULT;

  static const std::vector<std::string>
      SUBWORKLOAD_OPERATIONCOUNT_PROPERTY_LIST;
  static const std::string SUBWORKLOAD_OPERATIONCOUNT_DEFAULT;

  MixedWorkload() = default;

  void Init(utils::Properties &props, size_t thread_count = 1);

  ~MixedWorkload() {
    for (auto &i : subworkload_list) {
      delete i.second;
    }
  }
  inline void Reset() { last_id.store(0); }
  inline CoreWorkload *GetNext() {
    // if the type is single workload
    if (workload_type == WorkloadType::Single) {
      return &single_workload;
    }
    int subworkload_sum = subworkload_list.size();
    SubWorkloadType type =
        subworkload_ratio_table[last_id.load() % subworkload_sum];
    last_id.fetch_add(1);
    return subworkload_list[type];
  }
  std::string ToString() {
    std::string output_string;
    if (workload_type == WorkloadType::Single) {
      output_string.append("Workload Single;\t Thread Count : " +
                           std::to_string(single_workload.GetThreadCount()) +
                           "\n");
    }
    for (auto &i : subworkload_list) {
      std::string workload_type_name = SUBWORKLOAD_PROPERTY_LIST[i.first];
      std::string thread_count = std::to_string(i.second->GetThreadCount());
      output_string.append("Workload Mixed : " + workload_type_name +
                           ";\t Thread Count : " + thread_count + "\n");
    }
    return output_string;
  }

private:
  WorkloadType workload_type;
  // for single workload type
  CoreWorkload single_workload;
  // for mixed workload type
  std::atomic<int> last_id{0};
  std::vector<SubWorkloadType> subworkload_ratio_table;
  std::map<SubWorkloadType, ycsbc::CoreWorkload *> subworkload_list;
};

} // namespace ycsbc
