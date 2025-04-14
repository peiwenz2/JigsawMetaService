#include "MetaService.h"
#include <chrono>
#include <mutex>
#include <regex>
#include <vector>
#include <unordered_set>
#include <sstream>

void MetaService::parse_instance_key(const std::string& key, InstanceKeyInfo* keyInfo)
{
    std::string prefix, suffix, self_ip;
    try {
        prefix = config_["meta_data"]["instance"]["prefix"].get<std::string>();
        suffix = config_["meta_data"]["instance"]["suffix"].get<std::string>();
    } catch (const nlohmann::json::exception&) {
        throw std::invalid_argument("parse_instance_key failed: config_ decode failed");
    }
    // Function to escape regex special characters
    auto escape_regex = [](const std::string &s) {
        std::string escaped;
        for (char c : s) {
            if (c == '.' || c == '^' || c == '$' || c == '*' || c == '+' || c == '?' ||
                c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' ||
                c == '\\' || c == '|') {
                escaped += '\\';
            }
            escaped += c;
        }
        return escaped;
    };

    std::string escaped_prefix = escape_regex(prefix);
    std::string escaped_suffix = escape_regex(suffix);

    // matching the *instance* pattern
    std::string pattern_str = "^" + escaped_prefix + R"(\.([^.]+)\.instance\.(\d+)\.)" + escaped_suffix + "$";
    std::regex pattern;
    try {
        pattern = std::regex(pattern_str);
    } catch (const std::regex_error&) {
        throw std::invalid_argument("parse_instance_key failed: instance item decode failed");
    }

    std::smatch matches;
    if (std::regex_match(key, matches, pattern)) {
        if (matches.size() != 3)
        {
            throw std::invalid_argument("parse_instance_key failed: instance item does not match items");
        }

        std::string service_id = matches[1];
        std::string instance_num_str = matches[2];

        try {
            int instance_num = std::stoi(instance_num_str);
            keyInfo->prefix = prefix;
            keyInfo->suffix = suffix;
            keyInfo->service_id = service_id;
            keyInfo->instance_num = instance_num;
            keyInfo->ip = self_ip;
            return;
        } catch (const std::exception&) {
            throw std::invalid_argument("parse_instance_key failed: unknown error occur");
        }
    }
}

void MetaService::check_instance_health() {
    // get instance lists
    std::vector<std::string> scanned_keys = MetaServiceClient::GetKeysInSet("instanceinfo_keys");
    SPDLOG_LOGGER_INFO(logger, "Start check_instance_health. Instance found: {}", scanned_keys.size());

    // check if kv expire (value is empty)
    std::vector<std::string> aliveInstances;
    for (int i = 0; i < scanned_keys.size(); ++i) {
        std::string val = MetaServiceClient::Get(scanned_keys[i]); // e.g. instance-1744350470264-94
        if (!val.empty())
        {
            // instance alive
            aliveInstances.emplace_back(val);
        }
        else
        {
            InstanceKeyInfo info;
            parse_instance_key(scanned_keys[i], &info);
            SPDLOG_LOGGER_INFO(logger, "Found dead instance: {}, instance id: {}", scanned_keys[i], info.instance_num);
            MetaServiceClient::RemoveKeyFromSet("instanceinfo_keys", scanned_keys[i]);
        }
    }

    // write alive instances into a single kv
    MetaServiceClient::Set("alive_instance_list", VectorToStringWithComma(aliveInstances));
}