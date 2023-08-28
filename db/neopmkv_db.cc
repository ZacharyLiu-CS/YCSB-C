//
//  neopmkv_db.cc
//  PROJECT neopmkv_db
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "neopmkv_db.h"
#include "core/core_workload.h"
#include "db.h"
#include <vector>
#define LOGOUT(msg)                                                            \
  do {                                                                         \
    std::cerr << msg << std::endl;                                             \
    exit(0);                                                                   \
  } while (0)

namespace ycsbc {

NEOPMKV::NEOPMKV(const char *dbfilename) : no_found_(0) {
  std::string clean_cmd = std::string("rm -rf ") + std::string(dbfilename);
  int res = system(clean_cmd.c_str());

  std::string mkdir_cmd = std::string("mkdir -p ") + std::string(dbfilename);
  res = system(mkdir_cmd.c_str());

  ConfigReader config_reader = ConfigReader();
  neopmkv_config *nc =
      static_cast<neopmkv_config *>(config_reader.get_config("neopmkv").get());

  if (neopmkv_ == nullptr) {
    neopmkv_ = new NKV::NeoPMKV(
        dbfilename, nc->chunk_size_, nc->db_size_, nc->enable_pbrb_,
        nc->async_pbrb_, nc->async_gc_, nc->inplace_update_opt_,
        nc->max_page_num_, nc->rw_micro_, nc->gc_threshold_,
        nc->gc_interval_micro_, nc->hit_threshold_);
    enable_schema_aware_ = nc->enable_schema_aware_;
  }
  if (neopmkv_ == nullptr) {
    LOGOUT("init neopmkv failed!");
  }
}

Status NEOPMKV::Read(const std::string &table, const std::string &key,
                     const std::vector<std::string> *fields,
                     std::vector<KVPair> &result) {
  std::string value;
  std::vector<std::string> fields_value;
  NKV::SchemaId schemaId = key[3] - '0';

  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  // schema aware
  if (enable_schema_aware_ == true) {
    std::vector<uint32_t> fields_id;
    bool s;
    if (fields != nullptr) {
      for (auto &i : *fields) {
        fields_id.push_back(GetIntFromField(i));
      }
      // partial get
      s = neopmkv_->MultiPartialGet(read_key, fields_value, fields_id);
      return Status::kOK;
    }
    // full get
    s = neopmkv_->Get(read_key, value);
    if (s == false) {
      no_found_++;
      return Status::kErrorNoData;
    }
    return Status::kOK;
  }
  // schema no-aware
  auto s = neopmkv_->Get(read_key, value);
  if (s == false) {
    no_found_++;
    return Status::kErrorNoData;
  }
  auto t0 = Time::now();
  if (fields != nullptr) {
    DeserializeRowFilter(result, value, *fields);
  } else {
    DeserializeRow(result, value);
  }
  auto t1 = Time::now();
  read_des_count_.fetch_add(1);
  read_des_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
  return Status::kOK;
}

Status NEOPMKV::Scan(const std::string &table, const std::string &key, int len,
                     const std::vector<std::string> *fields,
                     std::vector<std::vector<KVPair>> &result) {
  std::vector<std::string> read_value;
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  uint64_t key_content = CoreWorkload::GetIntFromKey(key);

  // schema aware
  if (enable_schema_aware_ == true) {
    std::vector<uint32_t> fields_id;
    if (fields != nullptr) {
      for (auto &i : *fields) {
        fields_id.push_back(GetIntFromField(i));
      }
      // partial value access
      neopmkv_->PartialScan(read_key, read_value, len, fields_id[0]);
      return Status::kOK;
    }
    // full value access
    neopmkv_->Scan(read_key, read_value, len);
    return Status::kOK;
  }

  // schema no-aware
  neopmkv_->Scan(read_key, read_value, len);
  for (auto &v : read_value) {
    result.push_back(std::vector<KVPair>());
    std::vector<KVPair> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, v, *fields);
    } else {
      DeserializeRow(values, v);
    }
  }
  return Status::kOK;
}

Status NEOPMKV::Insert(const std::string &table, const std::string &key,
                       std::vector<KVPair> &values) {
  std::vector<NKV::Value> v;
  GetSecondElementToVec(values, v);
  for(auto &i : v){
    std::cout << i << "-----" << std::endl;
  }
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  auto s = neopmkv_->Put(read_key, v);
  return Status::kOK;
}

Status NEOPMKV::Update(const std::string &table, const std::string &key,
                       std::vector<KVPair> &values) {
  // first read values from db
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));
  std::vector<uint32_t> fields;
  std::vector<NKV::Value> fieldValues;
  GetFirstIntElementToVec(values, fields);
  GetSecondElementToVec(values, fieldValues);
  if (enable_schema_aware_ == true) {
    std::string value;
    // partial value update
    auto s = neopmkv_->MultiPartialUpdate(read_key, fieldValues, fields);
    if (s == false) {
      no_found_++;
      return Status::kErrorNoData;
    }
    return Status::kOK;
  }

  // then update the specific field
  std::string value;
  auto s = neopmkv_->Get(read_key, value);
  if (s == false) {
    no_found_++;
    return Status::kErrorNoData;
  }
  auto t0 = Time::now();
  std::vector<KVPair> current_values;
  DeserializeRow(current_values, value);
  auto t1 = Time::now();
  for (auto &new_field : values) {
    bool found = false;
    for (auto &current_field : current_values) {
      if (current_field.first == new_field.first) {
        found = true;
        current_field.second = new_field.second;
        break;
      }
    }
    if (found == false) {
      break;
    }
  }
  value.clear();
  SerializeRow(current_values, value);
  auto t2 = Time::now();
  update_des_count_.fetch_add(1);
  update_ser_count_.fetch_add(1);
  update_ser_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
  update_des_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

  s = neopmkv_->Put(read_key, value);
  return Status::kOK;
}

Status NEOPMKV::Delete(const std::string &table, const std::string &key) {
  NKV::SchemaId schemaId = key[3] - '0';
  NKV::Key read_key(schemaId, CoreWorkload::GetIntFromKey(key));

  auto s = neopmkv_->Remove(read_key);

  return Status::kOK;
}

void NEOPMKV::printStats() {
  std::cout << "print neopmkv statistics: " << std::endl;
  std::cout << "Missing operations count : " << no_found_ << std::endl;
  if (update_ser_count_.load() != 0) {
    std::cout << "Update serialization count: " << update_ser_count_.load()
              << " Avarage latency (nanoseconds): "
              << update_ser_sum_.load() / update_ser_count_.load() << std::endl;
  }
  if (update_des_count_.load() != 0) {
    std::cout << "Update deserialization count: " << update_des_count_.load()
              << " Avarage latency (nanoseconds): "
              << update_des_sum_.load() / update_des_count_.load() << std::endl;
  }
  if (read_des_count_.load() != 0) {
    std::cout << "Read deserialization count: " << read_des_count_.load()
              << " Avarage latency (nanoseconds): "
              << read_des_sum_.load() / read_des_count_.load() << std::endl;
  }
}

NEOPMKV::~NEOPMKV() {
  printStats();
  delete neopmkv_;
}
} // namespace ycsbc
