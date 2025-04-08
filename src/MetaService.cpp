#include "MetaService.h"
#include "redis_go_wrapper.h"

#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <sched.h>
#include <unistd.h>

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#define SPDLOG_USE_SOURCE_LOCATIONS

using namespace std::chrono_literals;

MetaService::MetaService(const std::string& config_path,
            const std::string& self_ip,
            const std::vector<std::string>& members_ip,
            const nlohmann::json& etcd_config,
            const nlohmann::json& redis_config)
{

    self_ip_ = self_ip;
    members_ip_ = members_ip;
    etcd_config_ = etcd_config;
    etcd_endpoints = etcd_config.value("endpoints", std::vector<std::string>{});
    redis_hosts = redis_config.value("hosts", std::vector<std::string>{});

    // init log system
    initLog();

    // init redis
    GoRedisWrapper::Initialize(config_path);
    std::vector<std::string> keys = {"etcd_hosts", "redis_hosts"};
    std::vector<std::string> values = {VectorToStringWithComma(etcd_endpoints), VectorToStringWithComma(redis_hosts)};
    auto start_write = std::chrono::high_resolution_clock::now();
    if (!GoRedisWrapper::BatchWrite(keys, values))
    {
        throw std::invalid_argument("Redis is broken. Write failed");
    }
    auto start_read = std::chrono::high_resolution_clock::now();
    std::vector<std::string> results = GoRedisWrapper::BatchRead(keys);
    if (results[0] != VectorToStringWithComma(etcd_endpoints) ||
        results[1] != VectorToStringWithComma(redis_hosts))
    {
        throw std::invalid_argument("Redis is broken. Read failed");
    }
    auto end = std::chrono::high_resolution_clock::now();
    SPDLOG_LOGGER_INFO(logger, "Redis init. self ip: {}, redis hosts: {}. simple write cost {}us, simple read cost {}us",
        self_ip_,
        VectorToStringWithComma(redis_hosts),
        std::chrono::duration_cast<std::chrono::microseconds>(start_read - start_write).count(),
        std::chrono::duration_cast<std::chrono::microseconds>(end - start_read).count());

    // init etcd
    etcd_client = std::make_shared<etcd::Client>(VectorToStringWithComma(etcd_endpoints));
    etcd_client->rmdir(ELECTION_KEY, true).wait();

    // validation
    if (std::find(members_ip_.begin(), members_ip_.end(), self_ip_) == members_ip_.end())
    {
        throw std::invalid_argument("Self IP not in members list");
    }

    SPDLOG_LOGGER_INFO(logger, "ETCD init. self ip: {}, member ip: {}, etcd endpoints: {}",
                        self_ip_, VectorToStringWithComma(members_ip_), VectorToStringWithComma(etcd_endpoints));
}

void MetaService::Run()
{
    SPDLOG_LOGGER_INFO(logger, "MetaService started");

    start_election();

    // Main loop
    while (running)
    {
        if (is_leader)
        {
            SPDLOG_LOGGER_INFO(logger, "do something in Run Leader");
        } else {
            // Follower do something
            SPDLOG_LOGGER_INFO(logger, "do something in Run Follower");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(MAIN_PERIODICAL_TASK_TIME_MS));
    }

    election_thread.join();
    scan_thread.join();
}

MetaService::~MetaService()
{
    running = false;
    if (is_leader && etcd_client)
    {
        etcd_client->rm(ELECTION_KEY).get();
    }
    if (logger)
    {
        logger->flush();
    }
}

void MetaService::initLog()
{
    if (system("mkdir -p " META_SERVICE_DIR) != 0)
    {
        throw std::runtime_error("Failed to create log directory");
    }
    logger = spdlog::rotating_logger_mt("meta_logger",
        LOG_FILENAME, LOG_ROTATE_SIZE, 1);
    logger->set_level(spdlog::level::info);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%s:%#] %v");
    logger->flush_on(spdlog::level::info);
    SPDLOG_LOGGER_INFO(logger, "Log system init.");
}

std::string MetaService::getEtcdLeaderByCli()
{
    const std::string command = "ETCDCTL_API=2 etcdctl --endpoints=" + VectorToStringWithComma(etcd_endpoints) +" member list";
    std::string leader_ip;

    // Execute the command and capture output
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe)
    {
        SPDLOG_LOGGER_ERROR(logger, "Failed to run etcdctl command");
        return "";
    }

    char buffer[1024 * 1024];
    bool has_leader = false;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr)
    {
        std::string line(buffer);
        if (line.find("isLeader=true") != std::string::npos)
        {
            size_t client_urls_start = line.find("clientURLs=");
            if (client_urls_start != std::string::npos)
            {
                client_urls_start += 11; // Skip "clientURLs="
                size_t client_urls_end = line.find(",", client_urls_start);
                if (client_urls_end == std::string::npos)
                {
                    client_urls_end = line.length();
                }
                std::string client_url = line.substr(client_urls_start, client_urls_end - client_urls_start);

                size_t ip_start = client_url.find("//") + 2;
                size_t ip_end = client_url.find(":", ip_start);
                leader_ip = client_url.substr(ip_start, ip_end - ip_start);
                has_leader = true;
                break;
            }
        }
    }

    if (!has_leader)
    {
        SPDLOG_LOGGER_ERROR(logger, "Cannot get etcd master, raw etcd output:{}, command:{}", buffer, command);
    }

    pclose(pipe);
    return leader_ip;
}

// *************** this function is using etcd to do election by lease ***************
// in case ETCD self-election not stable, we could select leader by ourself
void MetaService::use_etcd_leader_loop()
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1, &cpuset); // bind CPU1
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset))
    {
        SPDLOG_LOGGER_ERROR(logger, "Failed to set CPU affinity");
    }

    while (running)
    {
        try {
            // get etcd member list
            auto member_list_resp = etcd_client->list_member().get();
            if (member_list_resp.error_code())
            {
                SPDLOG_LOGGER_ERROR(logger, "Failed to get member list: {}", member_list_resp.error_message());
                continue;
            }

            // get current etcd leader
            std::string new_leader_id = getEtcdLeaderByCli();

            if (new_leader_id != "" && new_leader_id != current_etcd_leader)
            {
                // leader changed
                SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: Leader changed. Old leader: {}. New leader: {}. Self ip: {}",
                    current_etcd_leader, new_leader_id, self_ip_);
                if (current_etcd_leader.find(self_ip_) != std::string::npos)
                {
                    is_leader = false; // become follower
                }
                else if (new_leader_id.find(self_ip_) != std::string::npos)
                {
                    is_leader = true; // become leader
                }
                current_etcd_leader = new_leader_id; // update latest leader
            }
        }
        catch (const std::exception& e)
        {
            SPDLOG_LOGGER_ERROR(logger, "Leader check error: {}", e.what());
        }

        // Leader can do something here
        if (is_leader)
        {
            SPDLOG_LOGGER_INFO(logger, "Leader performing tasks...");
        }
        else
        {
            SPDLOG_LOGGER_INFO(logger, "Follower performing tasks...");
        }

        std::this_thread::sleep_for(std::chrono::seconds(ETCD_LEADER_CHECK_TIME));
    }
}

void MetaService::leader_election_loop()
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset); // bind CPU0
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset))
    {
        SPDLOG_LOGGER_ERROR(logger, "Failed to set CPU affinity");
    }

    while (running)
    {
        try {
            // get leader in etcd kv
            auto get_resp = etcd_client->get(ELECTION_KEY).get();
            if (get_resp.error_code() || get_resp.value().as_string().empty())
            {
                // leader key not exist, campaign
                SPDLOG_LOGGER_INFO(logger, "No current leader found, attempting election. Id {} election_key {}", self_id, ELECTION_KEY);
                auto lease = etcd_client->leasegrant(LEASE_EXIST_TIME).get(); // lease exist for 10s
                if (lease.error_code())
                {
                    SPDLOG_LOGGER_ERROR(logger, "Lease creation failed: {}", lease.error_message());
                    SPDLOG_LOGGER_ERROR(logger, "This Node will be follower");
                    is_leader = false;
                } else {
                    etcdv3::Transaction txn;
                    // only create when leader key not exist, write current ip
                    txn.add_compare_create(ELECTION_KEY, 0);
                    txn.add_success_put(ELECTION_KEY, self_id, lease.value().lease());
                    auto txn_resp = etcd_client->txn(txn).get();
                    if (txn_resp.is_ok())
                    {
                        // becoming leader
                        is_leader = true;
                        current_etcd_leader = self_id;
                        current_lease_id = lease.value().lease();
                        SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: Become leader, lease ID {}", current_lease_id);
                    } else {
                        // not leader, get current leader
                        auto new_resp = etcd_client->get(ELECTION_KEY).get();
                        if (new_resp.is_ok())
                        {
                            std::string leader = new_resp.value().as_string();
                            if (leader != current_etcd_leader)
                            {
                                current_etcd_leader = leader;
                                bool new_is_leader = (current_etcd_leader == self_id);
                                if (new_is_leader != is_leader)
                                {
                                    is_leader = new_is_leader;
                                    SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: {}", is_leader ? "Become leader" : "Become follower");
                                }
                            }
                        }
                    }
                }
            } else {
                // leader key exist
                std::string leader = get_resp.value().as_string();
                if (leader != current_etcd_leader)
                {
                    current_etcd_leader = leader;
                    bool new_is_leader = (current_etcd_leader == self_id);
                    if (new_is_leader != is_leader)
                    {
                        is_leader = new_is_leader;
                        SPDLOG_LOGGER_INFO(logger, "OnLeaderChange: {}", is_leader ? "Become leader" : "Become follower");
                    }
                }
                // if this ip is leader, renew lease
                if (is_leader && current_lease_id != 0)
                {
                    etcd_client->leasekeepalive(current_lease_id).wait();
                }
            }
        } catch (const std::exception& e)
        {
            SPDLOG_LOGGER_ERROR(logger, "Election polling error: {}", e.what());
        }

        // Leader can do something here
        if (is_leader)
        {
            try {
                SPDLOG_LOGGER_INFO(logger, "Leader do something here :)");
            } catch (const std::exception& e)
            {
                SPDLOG_LOGGER_ERROR(logger, "Scan error: {}", e.what());
            }
        } else {
            // follower
            SPDLOG_LOGGER_INFO(logger, "Currently follower, leader = {}", current_etcd_leader);
        }

        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

void MetaService::start_election()
{
    switch(election_strategy)
    {
        case USE_ETCD_LEADER:
            election_thread = std::thread([this]()
            {
                use_etcd_leader_loop();
            });
            break;

        case SELF_ELECTION:
            election_thread = std::thread([this]()
            {
                leader_election_loop();
            });
            break;

        default:
            throw std::invalid_argument("Unknown election strategy");
    }
}

