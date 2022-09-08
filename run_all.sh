#!/bin/bash
db_list=(pmrocksdb pmemkv fastfair neopmkv utree listdb)
# db_list=(fastfair neopmkv utree listdb)
thread_count_list=(1 8)
workload_list=(workloada workloadb workloadc workloadd workloade workloadf)
# workload_list=(workloadd)
value_size_list=(8B 1KB)
data_set_dir=/home/kvgroup/zhenliu/cache_project/microbench/data_sets/ycsbc-output
# ./ycsbc -db fastfair -dbpath /mnt/pmem0/ycsb-fastfair -P ../workloads/workloada.spec -threads 1 > fastfair_1.log 2>&1

for value_size in ${value_size_list[*]};do
  for db in ${db_list[*]};do
    for workload in ${workload_list[*]};do
      for thread_count in ${thread_count_list[*]};do
        # echo the command 
        echo "numactl --cpunodebind=0 --membind=0 ./ycsbc -db ${db} -dbpath /mnt/pmem0/ycsb-${db} -P ../workloads/${workload}_${value_size}.spec -threads ${thread_count} > ${data_set_dir}/${db}_thread_${thread_count}_value_${value_size}_${workload}.log 2>&1"
        # run the command
        numactl --cpunodebind=0 --membind=0 ./ycsbc -db ${db} -dbpath /mnt/pmem0/ycsb-${db} -P ../workloads/${workload}_${value_size}.spec -threads ${thread_count} > ${data_set_dir}/${db}_thread_${thread_count}_value_${value_size}_${workload}.log 2>&1
        # remove the files
        rm -rf /mnt/pmem0/ycsb-${db}
        sleep 1
        echo $(date)
      done
    done
  done
done
