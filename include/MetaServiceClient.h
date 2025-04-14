// MetaServiceClient.h
#pragma once

#include <string>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include "common.h"

class MetaServiceClient {
public:
    // ==================== Initialization ====================
    static bool Initialize(const std::string& config_path);

    // ==================== Core Key-Value Operations ====================
    static std::string Get(const std::string& key);
    static void Set(const std::string& key, const std::string& value, const std::string& set_name="");
    static std::string ZReadScore(const std::string& zsetKey, const std::string& member);
    static std::vector<std::pair<std::string, double>> ZRead(const std::string& zsetKey, int topN);
    static void SingleZDelete(const std::string& key, const std::string& member);
    static void ZWrite(const std::string& zsetKey, const std::string& member, double score, const std::string& withSetName = "");
    static bool Delete(const std::string& key);
    static void RemoveKeyFromSet(const std::string& set_name, const std::string& key);

    // ==================== Batch Operations ====================
    static bool BatchWrite(
        const std::vector<std::string>& keys,
        const std::vector<std::string>& values
    );

    static std::vector<std::string> BatchRead(
        const std::vector<std::string>& keys
    );

    static bool BatchDelete(
        const std::vector<std::string>& keys
    );

    static bool BatchZWrite(
        const std::vector<std::string>& zsetKeys,
        const std::vector<std::string>& members,
        const std::vector<double>& scores
    );

    static std::unordered_map<std::string, std::vector<std::pair<std::string, double>>>
        BatchZRead(const std::vector<std::string>& zsetKeys);

    // ==================== Advanced Operations ====================
    static std::vector<std::string> ScanInstanceKeys(
        const std::string& pattern = "instance*",
        int batch_size = 1000
    );

    static std::vector<std::string> GetHottestKeys(
        const std::string& prefix = "instance*",
        int batch_size = 1000,
        int top_n = 10
    );

    static std::vector<std::string> GetKeysInSet(
        const std::string& set_name = "instanceinfo_keys"
    );

    static std::vector<std::string> GetAliveInstanceList();

private:
    // ==================== C Interop Helpers ====================
    static std::vector<const char*> ConvertToCArray(
        const std::vector<std::string>& vec
    );

    static bool HandleError(const char* error);
    static void FreeCString(char* str);
    static void FreeKeyArray(char** arr, int count);
};