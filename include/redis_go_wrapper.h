#pragma once

#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include "common.h"

extern "C" {
    char* Initialize(const char* config_json);
    char* SafeBatchWrite(const char** keys, const char** values, int count);
    char* SafeBatchRead(const char** keys, int count, char*** results);
    char* SafeBatchDelete(const char** keys, int count);
    char** ScanInstanceKeys(const char* match_pattern, int batch_size, int *count);
    void FreeKeyArray(char** arr, int count);
    void FreeStrings(char** arr, int count);
    void FreeScanResults(char** arr, int count);
}

class GoRedisWrapper {
public:
    static bool Initialize(const std::string& config_path)
    {
        auto error = ::Initialize(config_path.c_str());
        return handleError(error);
    }

    static bool BatchWrite(const std::vector<std::string>& keys,
                          const std::vector<std::string>& values)
    {
        auto c_keys = convertToCArray(keys);
        auto c_vals = convertToCArray(values);

        auto error = ::SafeBatchWrite(c_keys.data(), c_vals.data(),
                                    static_cast<int>(keys.size()));
        return handleError(error);
    }

    static std::vector<std::string> BatchRead(const std::vector<std::string>& keys)
    {
        if (keys.empty())
        {
            return {};
        }

        auto c_keys = convertToCArray(keys);
        char** results = nullptr;

        auto error = ::SafeBatchRead(c_keys.data(),
                                   static_cast<int>(keys.size()),
                                   &results);
        handleError(error);

        std::vector<std::string> output;
        output.reserve(keys.size());
        if (results != nullptr)
        {
            for (size_t i = 0; i < keys.size(); ++i)
            {
                output.emplace_back(results[i] ? results[i] : "");
            }
            FreeKeyArray(results, static_cast<int>(keys.size()));
        }
        return output;
    }

    static bool BatchDelete(const std::vector<std::string>& keys)
    {
        auto c_keys = convertToCArray(keys);
        char* error = ::SafeBatchDelete(c_keys.data(), static_cast<int>(keys.size()));
        return handleError(error);
    }

    // single delete
    static bool Delete(const std::string& key)
    {
        return BatchDelete({key});
    }


    static std::vector<std::string> ScanInstanceKeys(
        const std::string& pattern = "instance*",
        int batch_size = 1000)
    {
        int count = 0;
        char** keys = ::ScanInstanceKeys(pattern.c_str(), batch_size, &count);

        if (!keys || count == 0)
        {
            return {};
        }

        std::vector<std::string> result;
        result.reserve(count);

        for (int i = 0; i < count; ++i)
        {
            result.emplace_back(keys[i]);
        }

        // Free memory using the same count parameter
        ::FreeScanResults(keys, count);

        return result;
    }

private:
    // mem safe
    static std::vector<const char*> convertToCArray(const std::vector<std::string>& vec)
    {
        std::vector<const char*> arr;
        arr.reserve(vec.size());
        for (const auto& s : vec)
        {
            arr.push_back(s.c_str());
        }
        return arr;
    }

    static void freeCArray(const std::vector<const char*>& arr)
    {
        for (const char* ptr : arr)
        {
            delete[] ptr;
        }
    }

    // add error handling
    static bool handleError(const char* error)
    {
        if (error && strlen(error) > 0)
        {
            //SPDLOG_LOGGER_ERROR(logger, "Redis Error. Error {}", error);
            return false;
        }
        return true;
    }
};