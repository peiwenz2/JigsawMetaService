#pragma once

#include <etcd/Client.hpp>
#include <etcd/Value.hpp>
#include <etcd/Watcher.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/v3/Transaction.hpp>
#include <etcd/v3/action_constants.hpp>
#include <hiredis/hiredis.h>
#include <sw/redis++/redis++.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <atomic>
#include <random>
#include <thread>
#include <vector>
#include <unordered_set>

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO

#include <spdlog/spdlog.h>

class MetaService {
public:
    MetaService();
    ~MetaService();

    void run();

private:
    void init_etcd();
    void init_redis();
    void election_loop();
    void leader_election_loop();
    bool validate_value(const std::string& value);

    std::shared_ptr<etcd::Client> etcd_client;
    std::shared_ptr<etcd::KeepAlive> lease_keepalive;
    std::shared_ptr<sw::redis::RedisCluster> redis_cluster;
    std::shared_ptr<spdlog::logger> logger;
    
    std::atomic<bool> is_leader{false};
    std::atomic<bool> running{true};
    std::thread election_thread;
    std::thread scan_thread;
    
    std::vector<std::string> etcd_endpoints;
    std::unordered_set<std::string> monitor_keys;
    
    const std::string ELECTION_KEY = "/meta_service/leader";
    const std::string HEARTBEAT_KEY = "/meta_service/heartbeat";
    const size_t LOG_ROTATE_SIZE = 10*1024*1024; // 10MB
    
    std::string last_role = "follower";
    char *self_id;
    std::string current_etcd_leader;
    int current_lease_id = -1;
};