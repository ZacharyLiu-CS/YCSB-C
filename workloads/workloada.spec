# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=10000000
operationcount=10000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
insertstart=1
readproportion=1.0
updateproportion=0
scanproportion=0
insertproportion=0
fieldcount=10
fieldlength=84
requestdistribution=zipfian
zipfianconst=1.2
