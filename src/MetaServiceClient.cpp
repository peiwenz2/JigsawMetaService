// MetaServiceClient.cpp
#include "MetaServiceClient.h"
#include <cstring>

// Declare C functions from Go library
extern "C" {
    char* Initialize(const char* config_json);
    char* SingleRead(const char* key);
    int SingleWrite(const char* key, const char* value);
    char* SafeBatchWrite(const char** keys, const char** values, int count);
    char* SafeBatchRead(const char** keys, int count, char*** results);
    char* SafeBatchDelete(const char** keys, int count);
    char** ScanInstanceKeys(const char* match_pattern, int batch_size, int *count);
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

void MetaServiceClient::Set(const std::string& key, const std::string& value) {
    int ret = ::SingleWrite(key.c_str(), value.c_str());
    if (ret != 0) {
        throw std::runtime_error("Failed to write key: " + key);
    }
}

bool MetaServiceClient::Delete(const std::string& key) {
    return BatchDelete({key});
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