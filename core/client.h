//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "db.h"
#include "utils.h"
#include <atomic>
#include <memory>
#include <string>

namespace ycsbc {

class Client {
public:
  Client(DB *db, CoreWorkload *wl) : db_(db), workload_(wl) {}

  bool DoInsert();
  bool DoTransaction();

  virtual ~Client() {
    if (client_read_count_.load() != 0) {

      std::cout << "Client Read count: " << client_read_count_.load()
                << " Avarage read build key latency (nanoseconds): "
                << client_read_build_key_latency_sum_.load() /
                       client_read_count_.load()
                << std::endl;
      std::cout << " Avarage read engine key latency (nanoseconds): "
                << client_read_engine_latency_sum_.load() /
                       client_read_count_.load()
                << std::endl;
    }
  }

protected:
  int TransactionRead();
  int TransactionReadModifyWrite();
  int TransactionScan();
  int TransactionUpdate();
  int TransactionInsert();

  DB* db_;
  CoreWorkload *workload_;
  std::atomic<uint64_t> client_read_count_{0};
  std::atomic<uint64_t> client_read_build_key_latency_sum_{0};
  std::atomic<uint64_t> client_read_engine_latency_sum_{0};
};

inline bool Client::DoInsert() {
  std::string key = workload_->NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_->BuildValues(pairs);
  return (db_->Insert(workload_->NextTable(), key, pairs) == Status::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  switch (workload_->NextOperation()) {
  case READ:
    status = TransactionRead();
    break;
  case UPDATE:
    status = TransactionUpdate();
    break;
  case INSERT:
    status = TransactionInsert();
    break;
  case SCAN:
    status = TransactionScan();
    break;
  case READMODIFYWRITE:
    status = TransactionReadModifyWrite();
    break;
  default:
    throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == Status::kOK);
}

inline int Client::TransactionRead() {
  client_read_count_.fetch_add(1, std::memory_order_relaxed);
  typedef std::chrono::high_resolution_clock Time;
  auto t0 = Time::now();
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  std::vector<DB::KVPair> result;
  auto t1 = Time::now();

  client_read_build_key_latency_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count(),
      std::memory_order_relaxed);

  Status s;
  auto t2 = Time::now();
  if (!workload_->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(workload_->NextFieldName());
    s = db_->Read(table, key, &fields, result);
  } else {
    s = db_->Read(table, key, NULL, result);
  }
  auto t3 = Time::now();
  client_read_engine_latency_sum_.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count(),
      std::memory_order_relaxed);
  return s;
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(workload_->NextFieldName());
    db_->Read(table, key, &fields, result);
  } else {
    db_->Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_->write_all_fields()) {
    workload_->BuildValues(values);
  } else {
    workload_->BuildUpdate(values);
  }
  return db_->Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  int len = workload_->NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(workload_->NextFieldName());
    return db_->Scan(table, key, len, &fields, result);
  } else {
    return db_->Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_->write_all_fields()) {
    workload_->BuildValues(values);
  } else {
    workload_->BuildUpdate(values);
  }
  return db_->Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_->BuildValues(values);
  return db_->Insert(table, key, values);
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
