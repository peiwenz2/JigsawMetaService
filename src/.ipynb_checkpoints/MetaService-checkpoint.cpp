#include "MetaService.h"
#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <sched.h>
#include <unistd.h> 

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#define SPDLOG_USE_SOURCE_LOCATIONS

using namespace std::chrono_literals;

std::string vectorToStringWithComma(const std::vector<std::string>& vec) {
    std::string result;
    for (size_t i = 0; i < vec.size(); ++i) {
        result += vec[i];
        if (i != vec.size() - 1) { // Don't add a comma after the last element
            result += ", ";
        }
    }
    return result;
}

MetaService::MetaService() {
    // 初始化日志系统
    if (system("mkdir -p /opt/meta_service/") != 0) {
        throw std::runtime_error("Failed to create log directory");
    }
    logger = spdlog::rotating_logger_mt("meta_logger", 
               "/opt/meta_service/meta_service.LOG", LOG_ROTATE_SIZE, 1);
    logger->set_level(spdlog::level::info);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%s:%#] %v");
    logger->flush_on(spdlog::level::info); 
    
    // 读取IP列表
    std::ifstream file("./iplist");
    std::string ip;
    while(getline(file, ip)) {
        etcd_endpoints.emplace_back("http://" + ip + ":2379");
    }
    
    init_etcd();
    //init_redis();
    
    char hostname[256]{0};
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        throw std::runtime_error("Failed to get hostname");
    }
    self_id = hostname;
    SPDLOG_LOGGER_INFO(logger, "Node id: {}", self_id);
}

void MetaService::init_etcd() {
    std::string etcd_hosts = "http://172.30.0.73:2379,http://172.30.0.72:2379,http://172.30.0.71:2379";
    etcd_client = std::make_shared<etcd::Client>(etcd_hosts);
    SPDLOG_LOGGER_INFO(logger, "Using etcd hosts {}", etcd_hosts);
}

void MetaService::init_redis() {
    sw::redis::ConnectionOptions opts;
    opts.type = sw::redis::ConnectionType::TCP;
    for (const auto& ep : etcd_endpoints) {
        //opts.host = ep.substr(7, ep.find(':')-7); // 从http://剥离IP
        logger->info("Using ip {} to connect to redis", opts.host);
        break; // 使用第一个节点IP连接Redis集群
    }
    opts.port = 7000;
    opts.host = "172.30.0.73";
    redis_cluster = std::make_shared<sw::redis::RedisCluster>(opts);
}

void MetaService::leader_election_loop() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset); // 绑定到CPU0
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset)) {
        SPDLOG_LOGGER_ERROR(logger, "Failed to set CPU affinity");
    }
    
    int wait_round = 0;

    while (running) {
        try {
            // 定期获取 etcd 中 leader 的信息
            auto get_resp = etcd_client->get(ELECTION_KEY).get();
            if (get_resp.error_code() || get_resp.value().as_string().empty()) {
                // leader key 不存在，尝试发起选举
                SPDLOG_LOGGER_INFO(logger, "No current leader found, attempting election. Id {} election_key {}", self_id, ELECTION_KEY);
                auto lease = etcd_client->leasegrant(10).get(); // 租约存活 10 秒
                if (lease.error_code()) {
                    SPDLOG_LOGGER_ERROR(logger, "Lease creation failed: {}", lease.error_message());
                } else {
                    etcdv3::Transaction txn;
                    // 当 leader key 不存在时才能写入；成功后写入当前节点的 id
                    txn.add_compare_create(ELECTION_KEY, 0);
                    txn.add_success_put(ELECTION_KEY, self_id, lease.value().lease());
                    auto txn_resp = etcd_client->txn(txn).get();
                    if (txn_resp.is_ok()) {
                        // 成功竞选，成为 leader
                        is_leader = true;
                        current_etcd_leader = self_id;
                        current_lease_id = lease.value().lease();
                        SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: Become leader, lease ID {}", current_lease_id);
                    } else {
                        // 竞选失败，读取当前 leader 信息
                        auto new_resp = etcd_client->get(ELECTION_KEY).get();
                        if (new_resp.is_ok()) {
                            std::string leader = new_resp.value().as_string();
                            if (leader != current_etcd_leader) {
                                current_etcd_leader = leader;
                                bool new_is_leader = (current_etcd_leader == self_id);
                                if (new_is_leader != is_leader) {
                                    is_leader = new_is_leader;
                                    SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: {}", is_leader ? "Become leader" : "Become follower");
                                }
                            }
                        }
                    }
                }
            } else {
                // leader key已经存在，从 etcd 中获得当前 leader 标识
                std::string leader = get_resp.value().as_string();
                if (leader != current_etcd_leader) {
                    current_etcd_leader = leader;
                    bool new_is_leader = (current_etcd_leader == self_id);
                    if (new_is_leader != is_leader) {
                        is_leader = new_is_leader;
                        SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: {}", is_leader ? "Become leader" : "Become follower");
                    }
                }
                // 如果本节点是 leader，则尝试续约租约
                if (is_leader && current_lease_id != 0) {
                    etcd_client->leasekeepalive(current_lease_id).wait();
                }
            }
        } catch (const std::exception& e) {
            SPDLOG_LOGGER_ERROR(logger, "Election polling error: {}", e.what());
        }
    
        // 根据最新确定的角色执行任务
        if (is_leader) {
            try {
                SPDLOG_LOGGER_INFO(logger, "Leader do something here :)");
            } catch (const std::exception& e) {
                SPDLOG_LOGGER_ERROR(logger, "Scan error: {}", e.what());
            }
        } else {
            // follower 状态下可以执行其它定期任务（例如仅确认 leader 状态）
            SPDLOG_LOGGER_INFO(logger, ("Currently follower, leader = {}", current_etcd_leader));
        }

        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

bool MetaService::validate_value(const std::string& value) {
    // 添加具体的验证逻辑
    return !value.empty();
}

void MetaService::run() {
    SPDLOG_LOGGER_INFO(logger, "MetaService started");
    //election_thread = std::thread(&MetaService::election_loop, this);
    scan_thread = std::thread(&MetaService::leader_election_loop, this);
    
    // 主循环处理请求
    while (running) {
        if (is_leader) {
            // 处理读写请求
        }
        std::this_thread::sleep_for(100ms);
    }
    
    //election_thread.join();
    scan_thread.join();
}

MetaService::~MetaService() {
    running = false;
    if (is_leader && etcd_client) {
        etcd_client->rm(ELECTION_KEY).get();
    }
    if (logger) {
        logger->flush();  // 确保日志刷新
    }
}