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

char* MetaServiceClient_Set(const char* key, const char* value) {
    try {
        MetaServiceClient::Set(key, value);
        return nullptr;
    } catch(const exception& e) {
        return strdup(e.what());
    } catch(...) {
        return strdup("Unknown write error");
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