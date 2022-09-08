//
//  mixed_workload.cc
//  PROJECT mixed_workload
//
//  Created by zhenliu on 31/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "mixed_workload.h"
#include "core_workload.h"
#include <string>
#include <vector>

using std::string;
using std::vector;
using ycsbc::MixedWorkload;

const string MixedWorkload::WORKLOAD_TYPE_PROPERTY = "workload_type";
const string MixedWorkload::WORKLOAD_TYPE_DEFAULT = "single";

const vector<string> MixedWorkload::SUBWORKLOAD_PROPERTY_LIST = {
    "workloada.spec", "workloadb.spec", "workloadc.spec",
    "workloadd.spec", "workloade.spec", "workloadf.spec"};
const string MixedWorkload::SUBWORKLOAD_UNIT_DEFAULT = "0";

const std::vector<std::string>
    MixedWorkload::SUBWORKLOAD_RECORDCOUNT_PROPERTY_LIST = {
        "a_recordcount", "b_recordcount", "c_recordcount",
        "d_recordcount", "e_recordcount", "f_recordcount"};
const std::string MixedWorkload::SUBWORKLOAD_RECORDCOUNT_DEFAULT = "0";

const std::vector<std::string>
    MixedWorkload::SUBWORKLOAD_OPERATIONCOUNT_PROPERTY_LIST = {
        "a_operationcount", "b_operationcount", "c_operationcount",
        "d_operationcount", "e_operationcount", "f_operationcount"};

const std::string MixedWorkload::SUBWORKLOAD_OPERATIONCOUNT_DEFAULT = "0";

void MixedWorkload::Init(utils::Properties &props, size_t thread_count) {
  std::string type =
      props.GetProperty(WORKLOAD_TYPE_PROPERTY, WORKLOAD_TYPE_DEFAULT);

  // if the workload type is single workload
  if (type == "single") {
    workload_type = WorkloadType::Single;
    single_workload.Init(props, thread_count);
    // std::cout << "Single workload Record Count: " << single_workload.GetRecordCount()<< std::endl;
    // std::cout << "Single workload Operation Count: " << single_workload.GetOperationCount()<< std::endl;
    // std::cout << "Single workload Thread Count:" << single_workload.GetThreadCount() << std::endl;
    return;
  }
  // here is the mixed workload configuration
  for (uint32_t i = SubWorkloadType::WORKLOAD_A; i < SubWorkloadType::MAX_TYPE;
       i++) {

    // get the workload unit
    std::string subworkload_props_name = SUBWORKLOAD_PROPERTY_LIST[i];
    std::string workload_unit =
        props.GetProperty(subworkload_props_name, SUBWORKLOAD_UNIT_DEFAULT);

    uint32_t workload_unit_count = std::stoi(workload_unit);
    // read the sub workload property file
    if (workload_unit_count != 0) {
      CoreWorkload *sub_workload = new CoreWorkload();
      subworkload_list[SubWorkloadType(i)] = sub_workload;
      // get the subworkload type and file path
      std::string basic_path = props.GetProperty("propsfile");
      int pos = basic_path.rfind("/") + 1;
      std::string subworkload_path =
          basic_path.substr(0, pos).append(subworkload_props_name);

      utils::Properties tmp_props;
      std::ifstream tmp_ifstream(subworkload_path);
      tmp_props.Load(tmp_ifstream);

      tmp_props.SetProperty(
          CoreWorkload::RECORD_COUNT_PROPERTY,
          props.GetProperty(SUBWORKLOAD_RECORDCOUNT_PROPERTY_LIST[i],
                            SUBWORKLOAD_RECORDCOUNT_DEFAULT));
      tmp_props.SetProperty(
          CoreWorkload::OPERATION_COUNT_PROPERTY,
          props.GetProperty(SUBWORKLOAD_OPERATIONCOUNT_PROPERTY_LIST[i],
                            SUBWORKLOAD_OPERATIONCOUNT_DEFAULT));
      sub_workload->Init(tmp_props, 0);
      sub_workload->SetWorkloadType(std::string("T") +
                                 subworkload_props_name.at(0) + "S0");
      tmp_ifstream.close();
    }
    // insert into the ratio table
    // exmaple: if workloada is 2, workload is 3
    // the subworkload_ration_table: [WORKLOAD_A, WORKLOAD_A,
    //                               WORKLOAD_D, WORKLOAD_D, WORKLOAD_D]
    for (uint32_t count = 0; count < workload_unit_count; count++)
      subworkload_ratio_table.push_back(SubWorkloadType(i));
  }
  for (auto i = 0; i < thread_count; i++) {
    auto core_workload = this->GetNext();
    core_workload->AddThreadCount(1);
  }
  this->Reset();
}