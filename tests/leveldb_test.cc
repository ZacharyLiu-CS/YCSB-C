#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include "utils.h"

#include "basic_db.h"
#include "core/db.h"
#include "db/db_factory.h"
#include "db/leveldb_db.h"

TEST(LevelDBTest, TestOperationInsert){

}

TEST(LevelDBTest, TestOperationRead){
  auto dbptr = std::make_shared<ycsbc::LevelDB>("tmp/testdb");
  //ASSERT_EQ(ycsbc::DB::kOK, dbptr->Insert());
}

TEST(LevelDBTest, TestOperationScan){

}

TEST(LevelDBTest, TestOperationDelete){

}





int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
