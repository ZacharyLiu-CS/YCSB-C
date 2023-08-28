//
//  config_reader.h
//  YCSB-C
//
//  Created by Zhen Liu on 19/01/2021.
//  Copyright (c) 2021 ZhenLiu <liuzhenm@mail.ustc.edu.cn>.
//
//
#ifndef YCSB_C_CONFIG_READER_H
#define YCSB_C_CONFIG_READER_H

#include "yaml-cpp/node/parse.h"
#include "yaml-cpp/yaml.h"
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <yaml-cpp/node/node.h>

namespace ycsbc {

inline bool convert_to_string(std::string &target_size, uint64_t base_size) {
  target_size.clear();
  if (base_size >= 1 << 30) {
    uint64_t prefix = base_size >> 30;
    target_size += std::to_string(prefix);
    target_size += "GB";
  } else if (base_size >= 1 << 20) {
    uint64_t prefix = base_size >> 20;
    target_size += std::to_string(prefix);
    target_size += "MB";
  } else {
    return false;
  }
  return true;
}
inline uint64_t convert_to_base(std::string &target_size) {
  size_t position_GB = target_size.find("GB");
  size_t position_MB = target_size.find("MB");
  uint64_t base_size = 0;
  if (position_GB != std::string::npos) {
    double prefix = std::stod(target_size.substr(0, target_size.size() - 2));
    base_size += prefix * (1 << 30);
  }
  if (position_MB != std::string::npos) {
    double prefix = std::stod(target_size.substr(0, target_size.size() - 2));
    base_size += prefix * (1 << 20);
  }
  return base_size;
}
struct config_templates {};
// config for leveldb and rocksdb
struct db_config : public config_templates {
  uint64_t bloom_bits_;
  bool enable_direct_io_;
  bool enable_compaction_;
  uint64_t thread_compaction_;
  uint64_t block_cache_size_;
  uint64_t memtable_size_;
  uint64_t sst_file_size_;
  uint64_t db_size_;
};

// config for pmemkv
struct pmemkv_config : public config_templates {
  bool create_if_missing_;
  uint64_t db_size_;
  std::string engine_type_;
};

// config for fastfair
struct fastfair_config : public config_templates {
  uint64_t db_size_;
};
// other ...

struct listdb_config : public config_templates {
  uint64_t pool_size_;
};

struct utree_config : public config_templates {
  uint64_t db_size_;
};


struct neopmkv_config : public config_templates {
  uint64_t chunk_size_;
  uint64_t db_size_;
  bool enable_schema_aware_;
  bool enable_pbrb_;
  bool async_pbrb_;
  bool async_gc_;
  bool inplace_update_opt_;
  uint64_t max_page_num_;
  uint64_t rw_micro_;
  double gc_threshold_;
  uint64_t gc_interval_micro_;
  double hit_threshold_;
};

} // namespace ycsbc

namespace YAML {
// encode and decode for db_config
// struct db_config : public config_templates {
//   uint64_t bloom_bits_;
//   bool enable_direct_io_;
//   bool enable_compaction_;
//   uint64_t thread_compaction_;
//   uint64_t block_cache_size_;
//   uint64_t memtable_size_;
//   uint64_t sst_file_size_;
//   uint64_t db_size_;
// };
template <> struct convert<ycsbc::db_config> {
  static YAML::Node encode(const ycsbc::db_config &dc) {
    Node node;
    node.push_back(dc.bloom_bits_);
    node.push_back(dc.enable_direct_io_);
    node.push_back(dc.enable_compaction_);
    node.push_back(dc.thread_compaction_);
    std::string block_cache_size;
    std::string memtable_size;
    std::string sst_file_size;
    std::string db_size;
    ycsbc::convert_to_string(block_cache_size, dc.block_cache_size_);
    ycsbc::convert_to_string(memtable_size, dc.memtable_size_);
    ycsbc::convert_to_string(sst_file_size, dc.sst_file_size_);
    ycsbc::convert_to_string(db_size, dc.db_size_);
    node.push_back(block_cache_size);
    node.push_back(memtable_size);
    node.push_back(sst_file_size);
    node.push_back(db_size);
    return node;
  }

  static bool decode(const Node &node, ycsbc::db_config &dc) {
    if (!node.IsMap()) {
      return false;
    }
    dc.bloom_bits_ = node["bloom_bits"].as<uint64_t>();
    dc.enable_direct_io_ = node["enable_direct_io"].as<bool>();
    dc.enable_compaction_ = node["enable_compaction"].as<bool>();
    dc.thread_compaction_ = node["thread_compaction"].as<uint64_t>();
    std::string block_cache_size = node["block_cache_size"].as<std::string>();
    std::string memtable_size = node["memtable_size"].as<std::string>();
    std::string sst_file_size = node["sst_file_size"].as<std::string>();
    std::string db_size = node["db_size"].as<std::string>();
    dc.block_cache_size_ = ycsbc::convert_to_base(block_cache_size);
    dc.memtable_size_ = ycsbc::convert_to_base(memtable_size);
    dc.sst_file_size_ = ycsbc::convert_to_base(sst_file_size);
    dc.db_size_ = ycsbc::convert_to_base(db_size);
    return true;
  }
};
// encode and decode for pmemkv_config
// struct fastfair_config : public config_templates {
//   uint64_t db_size_;
// };
template <> struct convert<ycsbc::pmemkv_config> {
  static YAML::Node encode(const ycsbc::pmemkv_config &pc) {
    Node node;
    std::string db_size;
    ycsbc::convert_to_string(db_size, pc.db_size_);
    // push back all config
    node.push_back(pc.create_if_missing_);
    node.push_back(db_size);
    node.push_back(pc.engine_type_);
    return node;
  }

  static bool decode(const Node &node, ycsbc::pmemkv_config &pc) {
    if (!node.IsMap()) {
      return false;
    }
    std::string db_size = node["db_size"].as<std::string>();
    pc.create_if_missing_ = node["create_if_missing"].as<bool>();
    pc.db_size_ = ycsbc::convert_to_base(db_size);
    pc.engine_type_ = node["engine_type"].as<std::string>();
    return true;
  }
};
// encode and decode for fastfair_config
template <> struct convert<ycsbc::fastfair_config> {
  static YAML::Node encode(const ycsbc::fastfair_config &fc) {
    Node node;
    std::string db_size;
    ycsbc::convert_to_string(db_size, fc.db_size_);
    // push back all config
    node.push_back(db_size);
    return node;
  }

  static bool decode(const Node &node, ycsbc::fastfair_config &fc) {
    if (!node.IsMap()) {
      return false;
    }
    std::string db_size = node["db_size"].as<std::string>();
    fc.db_size_ = ycsbc::convert_to_base(db_size);
    return true;
  }
};

// struct listdb_config : public config_templates {
//   uint64_t pool_size_;
// };

template <> struct convert<ycsbc::listdb_config> {
  static YAML::Node encode(const ycsbc::listdb_config &lc) {
    Node node;
    std::string pool_size;
    ycsbc::convert_to_string(pool_size, lc.pool_size_);
    // push back all config
    node.push_back(pool_size);
    return node;
  }

  static bool decode(const Node &node, ycsbc::listdb_config &lc) {
    if (!node.IsMap()) {
      return false;
    }
    std::string pool_size = node["pool_size"].as<std::string>();
    lc.pool_size_ = ycsbc::convert_to_base(pool_size);
    return true;
  }
};

// struct utree_config : public config_templates {
//   uint64_t db_size_;
// };
template <> struct convert<ycsbc::utree_config> {
  static YAML::Node encode(const ycsbc::utree_config &uc) {
    Node node;
    std::string db_size;
    ycsbc::convert_to_string(db_size, uc.db_size_);
    // push back all config
    node.push_back(db_size);
    return node;
  }

  static bool decode(const Node &node, ycsbc::utree_config &uc) {
    if (!node.IsMap()) {
      return false;
    }
    std::string db_size = node["db_size"].as<std::string>();
    uc.db_size_ = ycsbc::convert_to_base(db_size);
    return true;
  }
};

// end of encode and decode
// struct neopmkv_config : public config_templates {
//   uint64_t chunk_size_;
//   uint64_t db_size_;
//   bool enable_schema_aware_;
//   bool enable_pbrb_;
//   bool async_pbrb_;
//   bool async_gc_;
//   uint64_t max_page_num_;
//   uint64_t rw_micro_;
//   double gc_threshold_;
//   uint64_t gc_interval_micro_;
//   double hit_threshold_;
// };
template <> struct convert<ycsbc::neopmkv_config> {
  static YAML::Node encode(const ycsbc::neopmkv_config &nc) {
    Node node;
    std::string chunk_size;
    ycsbc::convert_to_string(chunk_size, nc.chunk_size_);
    node.push_back(chunk_size);
    std::string db_size;
    ycsbc::convert_to_string(db_size, nc.db_size_);
    // push back all config
    node.push_back(db_size);
    node.push_back(nc.enable_pbrb_);
    node.push_back(nc.enable_schema_aware_);
    node.push_back(nc.async_pbrb_);
    node.push_back(nc.async_gc_);
    node.push_back(nc.inplace_update_opt_);
    node.push_back(nc.max_page_num_);
    node.push_back(nc.rw_micro_);
    node.push_back(nc.gc_threshold_);
    node.push_back(nc.gc_interval_micro_);
    node.push_back(nc.hit_threshold_);
    return node;
  }

  static bool decode(const Node &node, ycsbc::neopmkv_config &nc) {
    if (!node.IsMap()) {
      return false;
    }
    std::string chunk_size = node["chunk_size"].as<std::string>();
    nc.chunk_size_ = ycsbc::convert_to_base(chunk_size);
    std::string db_size = node["db_size"].as<std::string>();
    nc.db_size_ = ycsbc::convert_to_base(db_size);
    nc.enable_pbrb_ = node["enable_pbrb"].as<bool>();
    nc.enable_schema_aware_ = node["enable_schema_aware"].as<bool>();
    nc.async_pbrb_ = node["async_pbrb"].as<bool>();
    nc.async_gc_ = node["async_gc"].as<bool>();
    nc.inplace_update_opt_= node["inplace_update_opt"].as<bool>();
    nc.max_page_num_ = node["max_page_num"].as<uint64_t>();
    nc.rw_micro_ = node["rw_micro"].as<uint64_t>();
    nc.gc_threshold_ = node["gc_threshold"].as<double>();
    nc.gc_interval_micro_ = node["gc_interval_micro"].as<uint64_t>();
    nc.hit_threshold_ = node["hit_threshold"].as<double>();
    return true;
  }
};

} // namespace YAML

namespace ycsbc {
class ConfigReader {
public:
  ConfigReader(std::string file_path = "../db_config.yaml") {
    this->load_config(file_path);
  }
  ~ConfigReader() {}
  bool load_config(std::string file_path = "../db_config.yaml") {
    YAML::Node lineup = YAML::LoadFile(file_path);
    for (auto it = lineup.begin(); it != lineup.end(); it++) {
      std::string dc_name = it->first.as<std::string>();
      if (dc_name == "leveldb" || dc_name == "pmrocksdb") {
        auto dc = std::make_shared<db_config>(it->second.as<db_config>());
        this->db_config_lists.insert(
            std::pair<std::string, std::shared_ptr<db_config>>(dc_name, dc));
      } else if (dc_name == "pmemkv") {
        auto pc =
            std::make_shared<pmemkv_config>(it->second.as<pmemkv_config>());
        this->db_config_lists.insert(
            std::pair<std::string, std::shared_ptr<pmemkv_config>>(dc_name,
                                                                   pc));
      } else if (dc_name == "fastfair") {
        auto fc =
            std::make_shared<fastfair_config>(it->second.as<fastfair_config>());
        this->db_config_lists.insert(
            std::pair<std::string, std::shared_ptr<fastfair_config>>(dc_name,
                                                                     fc));
      } else if (dc_name == "listdb") {
        auto lc =
            std::make_shared<listdb_config>(it->second.as<listdb_config>());
        this->db_config_lists.insert(
            std::pair<std::string, std::shared_ptr<listdb_config>>(dc_name,
                                                                   lc));
      } else if (dc_name == "utree") {
        auto uc = std::make_shared<utree_config>(it->second.as<utree_config>());
        this->db_config_lists.insert(
            std::pair<std::string, std::shared_ptr<utree_config>>(dc_name, uc));
      } else if (dc_name == "neopmkv") {
        auto nc =
            std::make_shared<neopmkv_config>(it->second.as<neopmkv_config>());
        this->db_config_lists.insert(
            std::pair<std::string, std::shared_ptr<neopmkv_config>>(dc_name,
                                                                    nc));
      }
    }
    return true;
  }
  std::shared_ptr<config_templates> get_config(const std::string &db_name) {
    auto it = this->db_config_lists.find(db_name);
    return it->second;
  }

private:
  std::map<std::string, std::shared_ptr<config_templates>> db_config_lists;
};

} // namespace ycsbc

#endif // YCSB_C_CONFIG_READER_H
