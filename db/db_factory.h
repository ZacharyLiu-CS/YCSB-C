//
//  db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/18/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_FACTORY_H_
#define YCSB_C_DB_FACTORY_H_

#include "core/db.h"
#include "core/properties.h"
#include "db/basic_db.h"
#include "db/leveldb_db.h"
#include "db/lock_stl_db.h"
#include "db/pmemkv_db.h"
#include "db/rocksdb_db.h"
#include "db/fastfair_db.h"
#include "db/neopmkv_db.h"
#include "db/utree_db.h"
#include "db/listdb_db.h"

#include <memory>
#include <string>
namespace ycsbc {

class DBFactory {
  public:
  static std::shared_ptr<DB> CreateDB(utils::Properties& props);
};

} // ycsbc

#endif // YCSB_C_DB_FACTORY_H_
