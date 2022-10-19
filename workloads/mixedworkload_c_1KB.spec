# Mixed of Yahoo! Cloud System Benchmark
# Workload type A: Update heavy workload,
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

workload_type=mixed

workloada.spec=1
a_recordcount=2500000
a_operationcount=2500000

workloadb.spec=1
b_recordcount=2500000
b_operationcount=2500000

workloadc.spec=1
c_recordcount=2500000
c_operationcount=2500000

workloadd.spec=1
d_recordcount=2500000
d_operationcount=2500000

workloade.spec=0
e_recordcount=2500000
e_operationcount=2500000

workloadf.spec=0
f_recordcount=2500000
f_operationcount=2500000

