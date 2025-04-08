#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <algorithm>

#include "redis_go_wrapper.h"

class RedisWrapperTest : public ::testing::Test {
protected:
    static constexpr const char* TEST_PREFIX = "testkey_";
    static constexpr int TEST_PORT = 7003;

    static std::vector<std::string> get_test_hosts() {
        return {
            "172.30.0.72:" + std::to_string(TEST_PORT),
            "172.30.0.72:" + std::to_string(TEST_PORT+1),
            "172.30.0.72:" + std::to_string(TEST_PORT+2)
        };
    }

    void SetUp() override {
        GoRedisWrapper::Initialize("./config_test.json");
    }

    void TearDown() override {
        fprintf(stderr, "Start TearDown\n");
        auto keys = GoRedisWrapper::ScanInstanceKeys("testkey*");
        if (!keys.empty()) {
            GoRedisWrapper::BatchDelete(keys);
            fprintf(stderr, "Case Tear Down. Cleared keys count %ld\n", keys.size());
        }
    }

    static std::string generate_test_key(const std::string& suffix) {
        return TEST_PREFIX + suffix + "_" + std::to_string(rand());
    }
};

TEST_F(RedisWrapperTest, BasicWriteRead) {
    fprintf(stderr, "Test BasicWriteRead start\n");
    std::vector<std::string> keys = {
        generate_test_key("basic1"),
        generate_test_key("basic2")
    };
    std::vector<std::string> values = {"value1", "value2"};

    ASSERT_NO_THROW(GoRedisWrapper::BatchWrite(keys, values));
    fprintf(stderr, "BatchWrite done\n");

    auto results = GoRedisWrapper::BatchRead(keys);
    fprintf(stderr, "BatchRead done\n");
    ASSERT_EQ(results.size(), keys.size());
    EXPECT_EQ(results[0], "value1");
    EXPECT_EQ(results[1], "value2");
}

TEST_F(RedisWrapperTest, ScanKeys) {
    const int TEST_KEY_COUNT = 1000;
    std::vector<std::string> keys;
    std::vector<std::string> values;

    for (int i = 0; i < TEST_KEY_COUNT; ++i) {
        keys.push_back(generate_test_key("scan_" + std::to_string(i)));
        values.push_back("scan_value_" + std::to_string(i));
    }

    auto start_write = std::chrono::high_resolution_clock::now();
    GoRedisWrapper::BatchWrite(keys, values);
    auto end_write = std::chrono::high_resolution_clock::now();
    //Batch write 1000, cost:3ms
    fprintf(stderr, "Batch write %d, cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write).count());

    auto start_scan = std::chrono::high_resolution_clock::now();
    auto scanned_keys = GoRedisWrapper::ScanInstanceKeys("scan");
    auto end_scan = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch scan %d, this failed cmd cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_scan - start_scan).count());
    ASSERT_EQ(scanned_keys.size(), 0); // prefix is wrong, should get nothing

    start_scan = std::chrono::high_resolution_clock::now();
    scanned_keys = GoRedisWrapper::ScanInstanceKeys("testkey_*");
    end_scan = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch scan %d, this success cmd cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_scan - start_scan).count());

    if (scanned_keys.size() != TEST_KEY_COUNT)
    {
        fprintf(stderr, "Retry once\n");
        start_scan = std::chrono::high_resolution_clock::now();
        scanned_keys = GoRedisWrapper::ScanInstanceKeys("testkey_*");
        end_scan = std::chrono::high_resolution_clock::now();
        fprintf(stderr, "Batch scan %d, this success cmd cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_scan - start_scan).count());
        ASSERT_GE(scanned_keys.size(), TEST_KEY_COUNT);
    }

    int matched = 0;
    for (const auto& key : scanned_keys) {
        if (std::find(keys.begin(), keys.end(), key) != keys.end()) {
            ++matched;
        }
    }
    EXPECT_EQ(matched, TEST_KEY_COUNT);
}

TEST_F(RedisWrapperTest, LargeBatchOperation) {
    const int LARGE_BATCH_SIZE = 5000;
    std::vector<std::string> keys;
    std::vector<std::string> values;

    for (int i = 0; i < LARGE_BATCH_SIZE; ++i) {
        keys.push_back(generate_test_key("large_" + std::to_string(i)));
        values.push_back("large_value_" + std::to_string(i));
    }

    auto start_write = std::chrono::high_resolution_clock::now();
    ASSERT_NO_THROW(GoRedisWrapper::BatchWrite(keys, values));
    auto end_write = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch write %d, cost:%ldms\n", LARGE_BATCH_SIZE, std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write).count());
    sleep(1);

    start_write = std::chrono::high_resolution_clock::now();
    auto results = GoRedisWrapper::BatchRead(keys);
    end_write = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch read %d, cost:%ldms\n", LARGE_BATCH_SIZE, std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write).count());
    ASSERT_EQ(results.size(), LARGE_BATCH_SIZE);

    for (int i = 0; i < LARGE_BATCH_SIZE; ++i) {
        EXPECT_EQ(results[i], values[i]);
    }
}

TEST_F(RedisWrapperTest, NonExistingKeys) {
    std::vector<std::string> keys = {
        generate_test_key("nonexist1"),
        generate_test_key("nonexist2")
    };

    auto results = GoRedisWrapper::BatchRead(keys);
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0].empty());
    EXPECT_TRUE(results[1].empty());
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}