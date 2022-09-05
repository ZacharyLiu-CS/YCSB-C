#!/bin/bash
db_list=(pmrocksdb pmemkv fastfair neopmkv utree listdb)
thread_count_list=(4)
workload_list=(workloada workloadb workloadc workloadd workloade workloadf)
value_size_list=(8B)


for value_size in ${value_size_list[*]};do
  for db in ${db_list[*]};do
    for workload in ${workload_list[*]};do
      for thread_count in ${thread_count_list[*]};do
       cat ${db}_thread_${thread_count}_value_${value_size}_${workload}.log | grep "Run Perf" | awk '{print$6}' >> ${db}_thread_${thread_count}_value_${value_size}.log
      done
    done
  done
done
