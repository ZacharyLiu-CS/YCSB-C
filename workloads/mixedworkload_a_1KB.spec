# Mixed of Yahoo! Cloud System Benchmark
# Workload type A: Update heavy workload,
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

workload_type=mixed

workloada.spec=1
a_recordcount=5000000
a_operationcount=5000000

workloadb.spec=1
b_recordcount=5000000
b_operationcount=5000000

workloadc.spec=0
c_recordcount=2000000
c_operationcount=2000000

workloadd.spec=0
d_recordcount=2000000
d_operationcount=2000000

workloade.spec=0
e_recordcount=2000000
e_operationcount=2000000

workloadf.spec=0
f_recordcount=2000000
f_operationcount=2000000

