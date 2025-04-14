// MetaServiceClient.cpp
#include "MetaServiceClient.h"
#include "common.h"
#include <cstring>

// Declare C functions from Go library
extern "C" {
    char* Initialize(const char* config_json);
    char* SingleRead(const char* key);
    int SingleWrite(const char* key, const char* value, const char* with_set_name);
    char* SingleZRead(const char* zsetKey, const char* member);
    char* SingleZReadRange(const char* zsetKey, int isTopN, int n);
    int SingleZWrite(const char* zsetKey, const char* member, double score, const char* withSetName);
    int SingleZRem(const char* zsetKey, const char* member);
    char* SafeBatchWrite(const char** keys, const char** values, int count);
    char* SafeBatchRead(const char** keys, int count, char*** results);
    char* SafeBatchDelete(const char** keys, int count);
    char* SafeBatchZWrite(const char** zsetKeys, const char** members, const double* scores, int count);
    char* SafeBatchZRead(const char** req, int count);
    int RemoveKeyFromSet(const char* setName, const char* key);
    char** ScanInstanceKeys(const char* match_pattern, int batch_size, int *count);
    char** GetKeysInSet(const char* set_name, int *count);
    char** GetHottestKeys(const char* prefix, int count, int batchSize, int *real_count);
    void FreeCString(char* key);
    void FreeKeyArray(char** arr, int count);
    void FreeStrings(char** arr, int count);
    void FreeScanResults(char** arr, int count);
}

// ==================== Initialization ====================
bool MetaServiceClient::Initialize(const std::string& config_path) {
    char* error = ::Initialize(config_path.c_str());
    return HandleError(error);
}

// ==================== Core Key-Value Operations ====================
std::string MetaServiceClient::Get(const std::string& key) {
    char* c_value = ::SingleRead(key.c_str());
    if (!c_value) {
        throw std::runtime_error("Failed to read key: " + key);
    }
    std::unique_ptr<char, decltype(&FreeCString)> guard(c_value, &FreeCString);
    return c_value;
}

void MetaServiceClient::Set(const std::string& key, const std::string& value, const std::string& set_name) {
    int ret = ::SingleWrite(key.c_str(), value.c_str(), set_name.c_str());
    if (ret != 0) {
        throw std::runtime_error("Failed to write key: " + key);
    }
}

bool MetaServiceClient::Delete(const std::string& key) {
    return BatchDelete({key});
}

void MetaServiceClient::RemoveKeyFromSet(const std::string& set_name, const std::string& key) {
    int ret = ::RemoveKeyFromSet(set_name.c_str(), key.c_str());
    if (ret != 0) {
        throw std::runtime_error("Failed to remove key: " + key + " from set: " + set_name);
    }
}

std::string MetaServiceClient::ZReadScore(const std::string& zsetKey, const std::string& member) {
    char* c_score = ::SingleZRead(zsetKey.c_str(), member.c_str());
    if (!c_score) {
        throw std::runtime_error("Failed to read zset member: " + zsetKey + ":" + member);
    }
    std::unique_ptr<char, decltype(&FreeCString)> guard(c_score, &FreeCString);
    //return std::stod(c_score);
    return c_score;
}

std::vector<std::pair<std::string, double>> MetaServiceClient::ZRead(const std::string& zsetKey, int topN) {
    char* members;
    if (topN == 0)
    {
        // return all
        members = ::SingleZReadRange(zsetKey.c_str(), topN, topN);
    }
    else
    {
        members = ::SingleZReadRange(zsetKey.c_str(), 1, topN);
    }
    if (!members) {
        throw std::runtime_error("Failed to read zset member woth key: " + zsetKey);
    }
    std::unique_ptr<char, decltype(&FreeCString)> guard(members, &FreeCString);
    nlohmann::json data = nlohmann::json::parse(members);
    std::vector<std::pair<std::string, double>> result;
    for (auto& item : data) {
        result.emplace_back(item["member"], item["score"]);
    }
    return result;
}

void MetaServiceClient::SingleZDelete(const std::string& key, const std::string& member) {
    int ret = ::SingleZRem(key.c_str(), member.c_str());
    if (ret != 0) {
        throw std::runtime_error("Failed to remove member: " + member + " from key zset: " + key);
    }
}

void MetaServiceClient::ZWrite(const std::string& zsetKey, const std::string& member, double score, const std::string& withSetName) {
    int ret = ::SingleZWrite(zsetKey.c_str(), member.c_str(), score, withSetName.c_str());
    if (ret != 0) {
        throw std::runtime_error("Failed to write zset member: " + zsetKey + ":" + member);
    }
}

// ==================== Batch Operations ====================
bool MetaServiceClient::BatchWrite(
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values
) {
    if (keys.size() != values.size()) {
        throw std::invalid_argument("Keys and values must have the same size");
    }
    auto c_keys = ConvertToCArray(keys);
    auto c_vals = ConvertToCArray(values);
    char* error = ::SafeBatchWrite(c_keys.data(), c_vals.data(), keys.size());
    return HandleError(error);
}

bool MetaServiceClient::BatchZWrite(
    const std::vector<std::string>& zsetKeys,
    const std::vector<std::string>& members,
    const std::vector<double>& scores
) {
    if (zsetKeys.size() != members.size() || members.size() != scores.size()) {
        throw std::invalid_argument("All input vectors must have the same size");
    }

    auto c_keys = ConvertToCArray(zsetKeys);
    auto c_members = ConvertToCArray(members);
    auto c_scores = scores.data();

    char* error = ::SafeBatchZWrite(
        c_keys.data(),
        c_members.data(),
        c_scores,
        static_cast<int>(zsetKeys.size())
    );
    return HandleError(error);
}

std::unordered_map<std::string, std::vector<std::pair<std::string, double>>>
MetaServiceClient::BatchZRead(const std::vector<std::string>& zsetKeys)
{
    auto cKeys = ConvertToCArray(zsetKeys);
    char* resultJson = ::SafeBatchZRead(cKeys.data(), zsetKeys.size());
    std::unique_ptr<char, decltype(&FreeCString)> guard(resultJson, &FreeCString);

    auto jsonResult = nlohmann::json::parse(resultJson);
    std::unordered_map<std::string, std::vector<std::pair<std::string, double>>> result;
    for (auto& [key, value] : jsonResult.items()) {
        std::vector<std::pair<std::string, double>> entries;
        for (auto& item : value) {
            entries.emplace_back(
                item["member"].get<std::string>(),
                item["score"].get<double>()
            );
        }
        result[key] = entries;
    }
    return result;
}

std::vector<std::string> MetaServiceClient::BatchRead(
    const std::vector<std::string>& keys
) {
    if (keys.empty()) return {};
    auto c_keys = ConvertToCArray(keys);
    char** results = nullptr;
    char* error = ::SafeBatchRead(c_keys.data(), keys.size(), &results);
    HandleError(error);

    std::vector<std::string> output;
    if (results) {
        output.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            output.emplace_back(results[i] ? results[i] : "");
        }
        FreeKeyArray(results, keys.size());
    }
    return output;
}

bool MetaServiceClient::BatchDelete(const std::vector<std::string>& keys) {
    auto c_keys = ConvertToCArray(keys);
    char* error = ::SafeBatchDelete(c_keys.data(), keys.size());
    return HandleError(error);
}

// ==================== Advanced Operations ====================
std::vector<std::string> MetaServiceClient::GetKeysInSet(const std::string& set_name)
{
    int count = 0;
    char** keys = ::GetKeysInSet(set_name.c_str(), &count);
    std::vector<std::string> result;
    if (keys && count > 0) {
        result.reserve(count);
        for (int i = 0; i < count; ++i) result.emplace_back(keys[i]);
        FreeScanResults(keys, count);
    }
    return result;
}

std::vector<std::string> MetaServiceClient::ScanInstanceKeys(
    const std::string& pattern,
    int batch_size
) {
    int count = 0;
    char** keys = ::ScanInstanceKeys(pattern.c_str(), batch_size, &count);
    std::vector<std::string> result;
    if (keys && count > 0) {
        result.reserve(count);
        for (int i = 0; i < count; ++i) result.emplace_back(keys[i]);
        FreeScanResults(keys, count);
    }
    return result;
}

std::vector<std::string> MetaServiceClient::GetHottestKeys(
    const std::string& prefix,
    int batch_size,
    int top_n
) {
    int count = 0;
    char** keys = ::GetHottestKeys(prefix.c_str(), batch_size, top_n, &count);
    std::vector<std::string> result;
    if (keys && count > 0) {
        result.reserve(count);
        for (int i = 0; i < count; ++i) result.emplace_back(keys[i]);
        FreeScanResults(keys, count);
    }
    return result;
}

// ==================== Specific function ====================
std::vector<std::string> MetaServiceClient::GetAliveInstanceList()
{
    return StringToVectorWithComma(Get("alive_instance_list"));
}

// ==================== Private Helpers ====================
std::vector<const char*> MetaServiceClient::ConvertToCArray(
    const std::vector<std::string>& vec
) {
    std::vector<const char*> arr;
    arr.reserve(vec.size());
    for (const auto& s : vec) arr.push_back(s.c_str());
    return arr;
}

bool MetaServiceClient::HandleError(const char* error) {
    if (error && strlen(error) > 0) {
        // Log error here if needed (e.g., SPDLOG_ERROR("Error: {}", error);)
        return false;
    }
    return true;
}

void MetaServiceClient::FreeCString(char* str) {
    ::FreeCString(str);  // Call the C function from Go library
}

void MetaServiceClient::FreeKeyArray(char** arr, int count) {
    ::FreeKeyArray(arr, count);  // Call the C function from Go library
}