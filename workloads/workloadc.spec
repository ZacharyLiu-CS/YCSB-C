# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=10000000
operationcount=100000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
insertstart=20000000

readproportion=0
updateproportion=0
scanproportion=1
insertproportion=0
requestdistribution=zipfian
maxscanlength=100
fieldcount=10
fieldlength=84
scanlengthdistribution=const
zipfianconst=1.2



