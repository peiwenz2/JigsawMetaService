#pragma once

#include <etcd/Client.hpp>
#include <etcd/Value.hpp>
#include <etcd/Watcher.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/v3/Transaction.hpp>
#include <etcd/v3/action_constants.hpp>
#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>
#include <sw/redis++/redis++.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <atomic>
#include <fstream>
#include <random>
#include <thread>
#include <vector>
#include <unordered_set>
#include "common.h"
#include "MetaServiceClient.h"

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO

#include <spdlog/spdlog.h>

enum ElectionStrategy {
    USE_ETCD_LEADER,
    SELF_ELECTION,
};

// instance info: e.g."prefix-key(serviceid.instance.num)-suffix"
struct InstanceKeyInfo {
    std::string prefix;
    std::string suffix;
    std::string service_id;
    int instance_num;
    std::string ip;
};

class MetaService {
public:
    MetaService(const std::string& config_path,
                const nlohmann::json& config);
    ~MetaService();

    void Run();

private:
    void initLog();
    void use_etcd_leader_loop();
    void leader_election_loop();

    void start_election();
    void check_instance_health();
    void parse_instance_key(const std::string& key, InstanceKeyInfo* keyInfo);
    std::string getEtcdLeaderByCli();

    std::shared_ptr<etcd::Client> etcd_client;
    std::shared_ptr<spdlog::logger> logger;

    std::atomic<bool> is_leader{false};
    std::atomic<bool> running{true};
    std::thread election_thread;
    std::thread scan_thread;

    ElectionStrategy election_strategy = USE_ETCD_LEADER;
    const std::string ELECTION_KEY = "/meta_service/leader";

    // for self-election only
    char *self_id;
    int current_lease_id = -1;

    nlohmann::json config_;
    std::vector<std::string> etcd_endpoints;
    std::vector<std::string> members_ip_;
    std::string self_ip_ = "127.0.0.1";
    std::string current_etcd_leader = "";

    // redis cluster
    std::vector<std::string> redis_hosts;
};