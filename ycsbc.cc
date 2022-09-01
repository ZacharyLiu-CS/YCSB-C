//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <algorithm>
#include <chrono>
#include <cstring>
#include <future>
#include <iostream>
#include <memory>
#include <ratio>
#include <string>
#include <vector>

#include "core/client.h"
#include "core/db.h"
#include "core/histogram.h"
#include "core/mixed_workload.h"
#include "core/properties.h"
#include "core/timer.h"
#include "core/utils.h"
#include "db/db_factory.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateClient(shared_ptr<ycsbc::DB> db, ycsbc::CoreWorkload *wl,
                   const int num_ops, bool is_loading,
                   shared_ptr<utils::Histogram> histogram) {
  utils::Timer<utils::t_microseconds> timer;
  ycsbc::Client client(db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    timer.Start();
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
    double duration = timer.End();
    histogram->Add_Fast(duration);
  }
  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);
  shared_ptr<ycsbc::DB> db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }
  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  ycsbc::MixedWorkload mixed_workload;
  mixed_workload.Init(props, num_threads);

  vector<future<int>> actual_ops;
  vector<shared_ptr<utils::Histogram>> histogram_list;
  utils::Timer<utils::t_microseconds> timer;

  int total_ops = 0;
  int sum = 0;
  if (props.GetProperty("skipload") != "true") {
    // Loads data
    db->Init();
    // calculate the total time usage
    timer.Start();

    for (int i = 0; i < num_threads; ++i) {
      auto histogram_tmp =
          make_shared<utils::Histogram>(utils::RecordUnit::h_microseconds);
      histogram_list.push_back(histogram_tmp);
      auto core_workload = mixed_workload.GetNext();
      int thread_ops =
          core_workload->GetRecordCount() / core_workload->GetThreadCount();
      actual_ops.emplace_back(async(launch::async, DelegateClient, db,
                                    core_workload, thread_ops, true,
                                    histogram_tmp));
      total_ops += thread_ops;
    }
    assert((int)actual_ops.size() == num_threads);

    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }

    double duration = timer.End();
    db->Close();

    utils::Histogram histogram(utils::RecordUnit::h_microseconds);
    for (auto &h : histogram_list) {
      histogram.Merge(*h);
    }
    cerr << "# Loading records:\t" << sum << endl;
    cerr << "Load Perf: " << props["dbname"] << '\t' << file_name << '\t'
         << num_threads << '\t';
    cerr << total_ops / (duration / histogram.GetRecordUnit()) << " OPS"
         << endl;
    cerr << histogram.ToString() << endl;
  } else {
    cerr << "# Skipped load records!" << endl;
  }

  // Peforms transactions
  actual_ops.clear();
  histogram_list.clear();
  mixed_workload.Reset();
  total_ops = 0;

  // calculate the total time usage
  timer.Start();
  for (int i = 0; i < num_threads; ++i) {
    auto histogram_tmp =
        make_shared<utils::Histogram>(utils::RecordUnit::h_microseconds);
    histogram_list.push_back(histogram_tmp);
    auto core_workload = mixed_workload.GetNext();
    int thread_ops =
        core_workload->GetOperationCount() / core_workload->GetThreadCount();
    actual_ops.emplace_back(async(launch::async, DelegateClient, db,
                                  core_workload, thread_ops, false,
                                  histogram_tmp));
    total_ops += thread_ops;
  }
  assert((int)actual_ops.size() == num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  auto duration = timer.End();

  utils::Histogram histogram(utils::RecordUnit::h_microseconds);
  for (auto &h : histogram_list) {
    histogram.Merge(*h);
  }

  db->Close();

  cerr << "# Transaction count:\t" << total_ops << endl;
  cerr << "Run Perf: " << props["dbname"] << '\t' << file_name << '\t'
       << num_threads << '\t';
  cerr << total_ops / (duration / histogram.GetRecordUnit()) << " OPS" << endl;
  cerr << histogram.ToString() << endl;
}

string ParseCommandLine(int argc, const char *argv[],
                        utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-dbpath") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbpath", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-skipload") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("skipload", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      props.SetProperty("propsfile", argv[argindex]);
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)"
       << endl;
  cout << "  -dbpath: specify the path of DB location" << endl;
  cout << "  -skipload: whether to run the load phase, sometime you can run on "
          "an existing database"
       << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple "
          "files can"
       << endl;
  cout << "                   be specified, and will be processed in the order "
          "specified"
       << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}
