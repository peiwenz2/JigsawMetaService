#include "MetaServiceClient.h"
#include <vector>
#include <cstring>
#include <stdexcept>

using namespace std;

// Helper function to convert C arrays to vectors
vector<string> convert_to_vector(const char** arr, int count) {
    vector<string> result;
    for(int i = 0; i < count; i++) {
        result.emplace_back(arr[i]);
    }
    return result;
}

vector<double> convert_double_to_vector(const double* arr, int count) {
    vector<double> result;
    for(int i = 0; i < count; i++) {
        result.emplace_back(arr[i]);
    }
    return result;
}

// Helper function to convert vectors to C arrays
char** convert_to_carray(const vector<string>& vec, int* count) {
    *count = vec.size();
    char** arr = (char**)malloc(*count * sizeof(char*));
    for(size_t i = 0; i < vec.size(); i++) {
        arr[i] = strdup(vec[i].c_str());
    }
    return arr;
}

extern "C" {
// ==================== Core API ====================
char* MetaServiceClient_Initialize(const char* config_path) {
    try {
        if(!MetaServiceClient::Initialize(config_path)) {
            return strdup("Initialization failed");
        }
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown initialization error");
    }
}

char* MetaServiceClient_Get(const char* key, char** value) {
    try {
        string result = MetaServiceClient::Get(key);
        *value = strdup(result.c_str());
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown read error");
    }
}

char* MetaServiceClient_Set(const char* key, const char* value, const char* set_name) {
    try {
        MetaServiceClient::Set(key, value, set_name);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown write error");
    }
}

char* MetaServiceClient_ZReadScore(const char* key, const char* member, char** value) {
    try {
        string result = MetaServiceClient::ZReadScore(key, member);
        *value = strdup(result.c_str());
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown read error");
    }
}

char* MetaServiceClient_ZReadRange(const char* key, int topN, char** value) {
    try {
        std::vector<std::pair<std::string, double>> result = MetaServiceClient::ZRead(key, topN);
        nlohmann::json j_array = nlohmann::json::array();
        for (const auto& item : result) {
            nlohmann::json j_item;
            j_item["member"] = item.first;
            j_item["score"] = item.second;
            j_array.push_back(j_item);
        }
        std::string json_str = j_array.dump();

        *value = strdup(json_str.c_str());
        if (!*value) {
            return strdup("Memory allocation failed");
        }
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown read error");
    }
}

char* MetaServiceClient_ZWrite(const char* key, const char* member, double score, const char* set_name) {
    try {
        MetaServiceClient::ZWrite(key, member, score, set_name);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown write error");
    }
}

char* MetaServiceClient_ZDelete(const char* key, const char* member) {
    try {
        MetaServiceClient::SingleZDelete(key, member);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown MetaServiceClient_ZDelete error");
    }
}

char* MetaServiceClient_Delete(const char* key) {
    try {
        if(!MetaServiceClient::Delete(key)) {
            return strdup("Delete operation failed");
        }
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown delete error");
    }
}

char* MetaServiceClient_RemoveKeyFromSet(const char* set_name, const char* key) {
    try {
        MetaServiceClient::RemoveKeyFromSet(set_name, key);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown RemoveKeyFromSet error");
    }
}

// ==================== Batch Operations ====================
char* MetaServiceClient_BatchWrite(const char** keys, const char** values, int count) {
    try {
        auto keys_vec = convert_to_vector(keys, count);
        auto values_vec = convert_to_vector(values, count);

        if(!MetaServiceClient::BatchWrite(keys_vec, values_vec)) {
            return strdup("Batch write failed");
        }
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown batch write error");
    }
}

char* MetaServiceClient_BatchRead(const char** keys, int count,
                                char*** results, int* result_count) {
    try {
        auto keys_vec = convert_to_vector(keys, count);
        auto values = MetaServiceClient::BatchRead(keys_vec);

        *results = convert_to_carray(values, result_count);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown batch read error");
    }
}

char* MetaServiceClient_BatchZRead(
    const char** keys,
    int count,
    char** result_json
) {
    try {
        std::vector<std::string> keys_vec;
        for (int i = 0; i < count; ++i) {
            keys_vec.emplace_back(keys[i]);
        }

        auto result = MetaServiceClient::BatchZRead(keys_vec);

        nlohmann::json json_result;
        for (const auto& [zset_key, entries] : result) {
            nlohmann::json j_entries = nlohmann::json::array();
            for (const auto& [member, score] : entries) {
                j_entries.push_back({
                    {"member", member},
                    {"score", score}
                });
            }
            json_result[zset_key] = j_entries;
        }

        std::string json_str = json_result.dump();
        *result_json = strdup(json_str.c_str());
        return nullptr;
    } catch (const std::exception& e) {
        return strdup(e.what());
    } catch (...) {
        return strdup("Unknown batch zread error");
    }
}

char* MetaServiceClient_BatchDelete(const char** keys, int count) {
    try {
        auto keys_vec = convert_to_vector(keys, count);
        if(!MetaServiceClient::BatchDelete(keys_vec)) {
            return strdup("Batch delete failed");
        }
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown batch delete error");
    }
}

char* MetaServiceClient_BatchZWrite(
    const char** zsetKeys,
    const char** members,
    const double* scores,
    int count
) {
    try {
        auto keys_vec = convert_to_vector(zsetKeys, count);
        auto members_vec = convert_to_vector(members, count);
        auto scores_vect = convert_double_to_vector(scores, count);
        if(!MetaServiceClient::BatchZWrite(keys_vec, members_vec, scores_vect)) {
            return strdup("Batch zset write failed");
        }
        return nullptr;
    } catch(const std::exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown batch ZWrite error");
    }
}

// ==================== Advanced Operations ====================
char* MetaServiceClient_ScanInstanceKeys(const char* pattern, int batch_size,
                                       char*** keys, int* key_count) {
    try {
        auto result = MetaServiceClient::ScanInstanceKeys(pattern, batch_size);
        *keys = convert_to_carray(result, key_count);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown scan error");
    }
}

char* MetaServiceClient_GetHottestKeys(const char* prefix, int batch_size, int top_n,
                                     char*** keys, int* key_count) {
    try {
        auto result = MetaServiceClient::GetHottestKeys(prefix, batch_size, top_n);
        *keys = convert_to_carray(result, key_count);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown hot keys error");
    }
}

char* MetaServiceClient_GetKeysInSet(const char* pattern, char*** keys, int* key_count) {
    try {
        auto result = MetaServiceClient::GetKeysInSet(pattern);
        *keys = convert_to_carray(result, key_count);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown scan error");
    }
}

// ==================== Specific function ====================
char* MetaServiceClient_GetAliveInstanceList(char*** keys, int* key_count) {
    try {
        std::vector<std::string> result = MetaServiceClient::GetAliveInstanceList();
        *keys = convert_to_carray(result, key_count);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown GetAliveInstanceList error");
    }
}

// ==================== Memory Management ====================
void MetaServiceClient_FreeString(char* str) {
    free(str);
}

void MetaServiceClient_FreeArray(char** arr, int count) {
    for(int i = 0; i < count; i++) {
        free(arr[i]);
    }
    free(arr);
}
} // extern "C"