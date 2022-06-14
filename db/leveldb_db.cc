//
//  leveldb_db.cc
//  YCSB-C
//
//  Created by zhenliu on 05/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "leveldb_db.h"

#include <iostream>
#include <vector>

#include "leveldb/iterator.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace ycsbc {

  LevelDB::LevelDB(const char *dbfilename) :no_found(0){
    Config_Reader config_reader = Config_Reader();
    db_config *dc = static_cast<db_config*>(config_reader.get_config("leveldb"));

    //create database if not exists
    options.create_if_missing = true;
    options.enable_direct_io = dc->enable_direct_io_;
    options.enable_compaction = dc->enable_compaction_;
    options.thread_compaction = dc->thread_compaction_;
    options.filter_policy = leveldb::NewBloomFilterPolicy(dc->bloom_bits_);
    options.block_cache = leveldb::NewLRUCache(dc->block_cache_size_);
    options.write_buffer_size = dc->memtable_size_;
    options.max_file_size = dc->sst_file_size_;
    leveldb::Status s = leveldb::DB::Open(options,dbfilename,&db_);
    if(!s.ok()){
      std::cerr << "init leveldb failed!"<<std::endl;
      exit(0);
    }
  }


  int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields, std::vector<KVPair> &result){
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(),key,&value);
    if ( s.IsNotFound() ){
      this->no_found ++;
      return DB::kErrorNoData;
    }else if ( !s.ok()){
      std::cerr << s.ToString() << std::endl;
      exit(0);
    }
    if ( fields != nullptr ){
      DeserializeRowFilter(result, value, *fields);
    }else{
      DeserializeRow(result, value);
    }
    return DB::kOK;
  }

  int LevelDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields, std::vector<std::vector<KVPair> > &result){
    leveldb::Iterator* iter = db_->NewIterator(leveldb::ReadOptions());
    iter->Seek(key);
    for( int i = 0; iter->Valid() && i < len; i++ ){
      std::string value = iter->value().ToString();
      result.push_back(std::vector<KVPair>());
      std::vector<KVPair> & values = result.back();
      if ( fields != nullptr ){
        DeserializeRowFilter(values, value, *fields);
      }else{
        DeserializeRow(values, value);
      }
      iter->Next();
    }
    delete iter;
    return DB::kOK;
  }
  int LevelDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values){
    std::string value;
    SerializeRow(values, value);

    leveldb::Status s;
    s = db_->Put(leveldb::WriteOptions(), key, value);
    if ( !s.ok() ){
      std::cerr << s.ToString() << std::endl;
      exit(0);
    }
    return DB::kOK;
  }

  int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values){
    // first read values from db
    std::string value;
    leveldb::Status s;
    s = db_->Get(leveldb::ReadOptions(), key, &value);
    if ( s.IsNotFound() ){
      this->no_found++;
      return DB::kErrorNoData;
    }else if( !s.ok() ){
      std::cerr << s.ToString() << std::endl;
      exit(0);
    }
    // then update the specific field
    std::vector<KVPair> current_values;
    DeserializeRow(current_values, value);
    for( auto& new_field: values ){
      bool found = false;
      for( auto& current_field: current_values ){
        if ( current_field.first == new_field.first ){
          found = true;
          current_field.second = new_field.second;
          break;
        }
      }
      if( found == false ){
        break;
      }
    }

    value.clear();
    SerializeRow(current_values, value);
    s = db_->Put(leveldb::WriteOptions(), key, value);
    if ( !s.ok() ){
      std::cerr << s.ToString() << std::endl;
      exit(0);
    }
    return DB::kOK;


  }

  int LevelDB::Delete(const std::string &table, const std::string &key){
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), key);
    if( !s.ok() ){
      std::cerr << s.ToString() << std::endl;
      exit(0);
    }
    return DB::kOK;
  }

  void LevelDB::printStats(){
    std::string stats;
    db_->GetProperty("leveldb.stats",&stats);
    std::cout << stats << std::endl;
    std::cout << "Missing operations count : " << this->no_found << std::endl;
  }

  LevelDB::~LevelDB(){
    std::cout << "print leveldb statistics: " << std::endl;
    printStats();
    delete db_;
  }

} //ycsbc
