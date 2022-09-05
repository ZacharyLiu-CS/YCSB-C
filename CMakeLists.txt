#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(ycsbc)

#3. specify c++ compiler version
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-std=c++17 -pthread")
add_definitions(-DON_DCPMM)

if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
  add_compile_definitions(ENABLE_DEBUG)
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -fopenmp")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

find_package(PkgConfig REQUIRED)
find_package(fmt REQUIRED)
pkg_check_modules(LIBPMEMKV REQUIRED libpmemkv)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)
pkg_check_modules(LIBPMEM REQUIRED libpmem)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
option(ENGINE_TREE3 "enable experimental tree3 engine" ON)

# enable test
option(BUILD_TESTS "Build test programs" ON)
# enable gperftools
option(ENABLE_GPERF "Enable google perftools" OFF)

# if the option is true, add compile definitions
if(ENABLE_GPERF)
  add_definitions(-DENABLE_GPERF)
endif(ENABLE_GPERF)


#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/core
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/include
  ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
  ${PROJECT_SOURCE_DIR}/db_impl/leveldb/include
  ${PROJECT_SOURCE_DIR}/db_impl/pmem-rocksdb/include
  ${PROJECT_SOURCE_DIR}/db_impl/FAST_FAIR/
  ${PROJECT_SOURCE_DIR}/db_impl/listdb/
  ${PROJECT_SOURCE_DIR}/db_impl/NeoPMKV/include
  ${PROJECT_SOURCE_DIR}/db_impl/utree
  ${LIBPMEMKV_INCLUDE_DIRS}
  ${LIBPMEMOBJ_INCLUDE_DIRS}
  )

#5. third path link path
link_directories(
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/build/
  ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib/
  ${PROJECT_SOURCE_DIR}/db_impl/leveldb/build/
  ${PROJECT_SOURCE_DIR}/db_impl/pmem-rocksdb/
  ${PROJECT_SOURCE_DIR}/db_impl/FAST_FAIR/concurrent_pmdk/
  ${PROJECT_SOURCE_DIR}/db_impl/listdb/build
  ${PROJECT_SOURCE_DIR}/db_impl/NeoPMKV/build
  ${PROJECT_SOURCE_DIR}/db_impl/utree
  ${LIBPMEMKV_LIBRARY_DIRS}
  ${LIBPMEMOBJ_LIBRARY_DIRS}
  ${LIBPMEM_LIBRARY_DIRS}
  )

#6. source code file path
file(GLOB MAIN_FILE
  ${PROJECT_SOURCE_DIR}/ycsbc.cc
  )
file(GLOB SOURCE_FILE
  ${PROJECT_SOURCE_DIR}/core/*.cc
  ${PROJECT_SOURCE_DIR}/db/*.cc
  )

#7. define the executable
add_executable(${PROJECT_NAME} ${MAIN_FILE} ${SOURCE_FILE})
target_link_libraries(${PROJECT_NAME}
  ${LIBPMEMOBJ_LIBRARIES}
  pmemobj
  pmem
  pthread
  yaml-cpp
  gtest
  leveldb
  rocksdb
  pmemkv
  fastfair
  utree
  listdb
  neopmkv
  snappy
  zstd
  lz4
  bz2
  z
  dl
  fmt::fmt
  profiler
  )


if(BUILD_TESTS)
  #8. define gtest path
  set(GTEST_LIBRARY ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib)
  set(GTEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
  set(GTEST_MAIN_LIBRARY ${GTEST_LIBRARY})

  #9. compile test files
  find_package(GTest)
  enable_testing()
  if (GTEST_FOUND)
    file(GLOB TEST_FILE ${PROJECT_SOURCE_DIR}/tests/*.cc)
    foreach(testsourcefile ${TEST_FILE})
      string(REGEX MATCH "[^/]+$" testsourcefilewithoutpath ${testsourcefile})
      string(REPLACE ".cc" "" testname ${testsourcefilewithoutpath})
      add_executable( ${testname} ${testsourcefile} ${SOURCE_FILE})
      target_link_libraries(${testname}
        ${LIBPMEMOBJ_LIBRARIES}
        pmemobj
        pmem
        pthread
        yaml-cpp
        gtest
        leveldb
        rocksdb
        pmemkv
        fastfair
        utree
        listdb
        neopmkv
        snappy
        zstd
        lz4
        bz2
        z
        dl
        fmt::fmt
        )
      add_test(NAME ${testname} COMMAND ${testname})
    ENDFOREACH(testsourcefile ${TEST_FILE})
  endif()
endif(BUILD_TESTS)


