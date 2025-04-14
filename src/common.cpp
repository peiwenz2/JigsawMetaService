#include "common.h"

std::string VectorToStringWithComma(const std::vector<std::string>& vec)
{
    std::string result;
    for (size_t i = 0; i < vec.size(); ++i)
    {
        result += vec[i];
        if (i != vec.size() - 1)
        {
            // Don't add a comma after the last element
            result += ",";
        }
    }
    return result;
}

std::vector<std::string> StringToVectorWithComma(const std::string& str) {
    std::vector<std::string> vec;
    std::stringstream ss(str);
    std::string token;

    while (std::getline(ss, token, ',')) {
        vec.push_back(token);
    }

    return vec;
}

std::vector<std::pair<std::string, double>> ParseZRangeResult(const char* json)
{
    nlohmann::json data = nlohmann::json::parse(json);
    std::vector<std::pair<std::string, double>> result;
    for (const auto& item : data)
    {
        result.emplace_back(item["Member"].get<std::string>(), item["Score"].get<double>());
    }
    return result;
}