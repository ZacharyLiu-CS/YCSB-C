# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian



recordcount=192000000
operationcount=1024000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

fieldlength=128
fieldcount=1

readproportion=0.875
updateproportion=0.125
scanproportion=0
insertproportion=0

requestdistribution=zipfian

