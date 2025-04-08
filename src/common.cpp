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