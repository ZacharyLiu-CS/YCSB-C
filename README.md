# YCSB-C ![build and test](https://github.com/ZacharyLiu-CS/YCSB-C/workflows/build%20and%20test/badge.svg)

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start
To init submodules

```
$ git submodule init
$ git submodule update
```
To install packages

```
$ sudo apt install libsnappy-dev libgflags-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libpthread-stubs0-dev
```

To build third-party and db imples
```
$ cd third-party && ./build_third_party.sh
$ cd db_impl && ./build_db_impl.sh
```

To build YCSB-C on Ubuntu, for example:

```
$ mkdir -p build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Release ..
$ cmake --build . -j
```

To run test:

```
$ cd build && ctest
```

Run Workload A with a leveldb
implementation of the database, for example:
```
$ cd build
$ ./ycsbc -db leveldb -dbpath . -P ../workloads/workloada.spec -threads 4
```
# New Features: Mixed Workload
Now we can configure one mixed workload to run serveral different single workload at the same time. The basic element is workload unit.
One workload unit can be executed at least one thread.

So there is the thread task allocation model.  
For example, assume that we have a mixed workload has `{ workloada: 1, workloadb:2, workloadc unit:3}`.   
So if we have 12 threads, we will allocate them like this: `thread 0 -> workloada; thread 1,2 -> workloadb; thread 3,4,5 -> workloadc; thread 6 -> workloada; thread 7,8 -> workloadb; thread 9,10 -> workloadc`.  
After all we can see each workload unit has two threads to execute, so each thread will execute halt of the recordcount/operationcount.
### Usage
```
$ cd build
$ ./ycsbc -db leveldb -dbpath . -P ../workloads/mixedworkload_template.spec -threads 6
```

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the recordcount property. Reference properties
files in the workloads dir.

