//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"
#include "basic_db.h"
#include "dssdb_db.h"
#include "lock_stl_db.h"
#include <memory>

namespace ycsbc {
  std::shared_ptr<DB> DBFactory::CreateDB(utils::Properties &props) {
    if (props["dbname"] == "basic") {
      return std::make_shared<BasicDB>();
    } else if (props["dbname"] == "lock_stl") {
      return std::make_shared<LockStlDB>();
   } else if (props["dbname"] == "dssdb") {
     return std::make_shared<DssDB>(props["host"], props["port"]);
    } else return NULL;
  }

}
