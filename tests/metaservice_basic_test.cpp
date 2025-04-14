#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <string>
#include <algorithm>

#include "MetaServiceClient.h"

class MetaServiceClientTest : public ::testing::Test {
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
        if (!MetaServiceClient::Initialize("./config_test.json")) {
            fprintf(stderr, "MetaServiceClient Initialize Failed\n");
        }
    }

    void TearDown() override {
        fprintf(stderr, "Start TearDown\n");
        auto keys = MetaServiceClient::ScanInstanceKeys("testkey*");
        if (!keys.empty()) {
            MetaServiceClient::BatchDelete(keys);
            fprintf(stderr, "Case Tear Down. Cleared keys count %ld\n", keys.size());
        }
    }

    static std::string generate_test_key(const std::string& suffix) {
        return TEST_PREFIX + suffix + "_" + std::to_string(rand());
    }
};

TEST_F(MetaServiceClientTest, SingleBasicTest) {
    // Set a key
    MetaServiceClient::Set("user:1001", R"({"name": "Alice", "role": "admin"})");

    // Get a key
    std::string value = MetaServiceClient::Get("user:1001");

    // Delete a key
    MetaServiceClient::Delete("user:1001");
}

TEST_F(MetaServiceClientTest, BasicWriteRead) {
    fprintf(stderr, "Test BasicWriteRead start\n");
    std::vector<std::string> keys = {
        generate_test_key("basic1"),
        generate_test_key("basic2")
    };
    std::vector<std::string> values = {"value1", "value2"};

    ASSERT_NO_THROW(MetaServiceClient::BatchWrite(keys, values));
    fprintf(stderr, "BatchWrite done\n");

    auto results = MetaServiceClient::BatchRead(keys);
    fprintf(stderr, "BatchRead done\n");
    ASSERT_EQ(results.size(), keys.size());
    EXPECT_EQ(results[0], "value1");
    EXPECT_EQ(results[1], "value2");
}

TEST_F(MetaServiceClientTest, ScanKeys) {
    const int TEST_KEY_COUNT = 1000;
    std::vector<std::string> keys;
    std::vector<std::string> values;

    for (int i = 0; i < TEST_KEY_COUNT; ++i) {
        keys.push_back(generate_test_key("scan_" + std::to_string(i)));
        values.push_back("scan_value_" + std::to_string(i));
    }

    auto start_write = std::chrono::high_resolution_clock::now();
    MetaServiceClient::BatchWrite(keys, values);
    auto end_write = std::chrono::high_resolution_clock::now();
    //Batch write 1000, cost:3ms
    fprintf(stderr, "Batch write %d, cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write).count());

    auto start_scan = std::chrono::high_resolution_clock::now();
    auto scanned_keys = MetaServiceClient::ScanInstanceKeys("scan");
    auto end_scan = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch scan %d, this failed cmd cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_scan - start_scan).count());
    ASSERT_EQ(scanned_keys.size(), 0); // prefix is wrong, should get nothing

    start_scan = std::chrono::high_resolution_clock::now();
    scanned_keys = MetaServiceClient::ScanInstanceKeys("testkey_*");
    end_scan = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch scan %d, this success cmd cost:%ldms\n", TEST_KEY_COUNT, std::chrono::duration_cast<std::chrono::milliseconds>(end_scan - start_scan).count());

    if (scanned_keys.size() != TEST_KEY_COUNT)
    {
        fprintf(stderr, "Retry once\n");
        start_scan = std::chrono::high_resolution_clock::now();
        scanned_keys = MetaServiceClient::ScanInstanceKeys("testkey_*");
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

TEST_F(MetaServiceClientTest, LargeBatchOperation) {
    const int LARGE_BATCH_SIZE = 5000;
    std::vector<std::string> keys;
    std::vector<std::string> values;

    for (int i = 0; i < LARGE_BATCH_SIZE; ++i) {
        keys.push_back(generate_test_key("large_" + std::to_string(i)));
        values.push_back("large_value_" + std::to_string(i));
    }

    auto start_write = std::chrono::high_resolution_clock::now();
    ASSERT_NO_THROW(MetaServiceClient::BatchWrite(keys, values));
    auto end_write = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch write %d, cost:%ldms\n", LARGE_BATCH_SIZE, std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write).count());
    sleep(1);

    start_write = std::chrono::high_resolution_clock::now();
    auto results = MetaServiceClient::BatchRead(keys);
    end_write = std::chrono::high_resolution_clock::now();
    fprintf(stderr, "Batch read %d, cost:%ldms\n", LARGE_BATCH_SIZE, std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write).count());
    ASSERT_EQ(results.size(), LARGE_BATCH_SIZE);

    for (int i = 0; i < LARGE_BATCH_SIZE; ++i) {
        EXPECT_EQ(results[i], values[i]);
    }
}

TEST_F(MetaServiceClientTest, NonExistingKeys) {
    std::vector<std::string> keys = {
        generate_test_key("nonexist1"),
        generate_test_key("nonexist2")
    };

    auto results = MetaServiceClient::BatchRead(keys);
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0].empty());
    EXPECT_TRUE(results[1].empty());
}

TEST_F(MetaServiceClientTest, GetHottestKeysBasic) {
    const std::string prefix = "hottest_";
    const int key_count = 5;
    const int top_n = 3;

    std::vector<std::string> test_keys;
    std::vector<std::string> test_values;
    for (int i = 1; i <= key_count; ++i) {
        test_keys.push_back(generate_test_key(prefix + "key" + std::to_string(i)));
        test_values.push_back("value" + std::to_string(i));
    }

    ASSERT_NO_THROW(MetaServiceClient::BatchWrite(test_keys, test_values));

    auto access_pattern = [&](){
        for (int i = 0; i < 500; ++i) {
            // need single read here
            MetaServiceClient::Get(test_keys[0]);
        }

        for (int i = 0; i < 200; ++i) {
            MetaServiceClient::Get(test_keys[1]);
        }

        for (int i = 0; i < 100; ++i) {
            MetaServiceClient::Get(test_keys[2]);
        }

        // key4,5: 0times
    };

    ASSERT_NO_THROW(access_pattern());

    // 3. Get hot key
    std::vector<std::string> hot_keys;
    ASSERT_NO_THROW({
        hot_keys = MetaServiceClient::GetHottestKeys(
            "testkey_" + prefix + "*",
            1000,
            top_n // get top 3
        );
    });

    EXPECT_EQ(hot_keys.size(), top_n) << "Should return exactly top N keys";

    if (hot_keys.size() >= 1) {
        EXPECT_EQ(hot_keys[0], test_keys[0])
            << "Most accessed key should be first";
    }
    if (hot_keys.size() >= 2) {
        EXPECT_EQ(hot_keys[1], test_keys[1])
            << "Second most accessed key should be second";
    }
    if (hot_keys.size() >= 3) {
        EXPECT_EQ(hot_keys[2], test_keys[2])
            << "Third most accessed key should be third";
    }
}

TEST_F(MetaServiceClientTest, GetKeysInSetTest) {
    std::vector<std::string> keys = {
        generate_test_key("getkeys")
    };
    std::vector<std::string> values = {
        "getkeys"
    };

    // write kv and add set
    MetaServiceClient::Set(keys[0], values[0], "test_set");

    // get keys by GetKeysInSetTest and check value
    std::vector<std::string> results = MetaServiceClient::GetKeysInSet("test_set");
    EXPECT_EQ(results.size(), keys.size());
    std::string val = MetaServiceClient::Get(results[0]);
    EXPECT_EQ(results[0], keys[0]);
    EXPECT_EQ(val, values[0]);

    // only delete the kv, the key should still in the set
    MetaServiceClient::Delete(keys[0]);
    results = MetaServiceClient::GetKeysInSet("test_set");
    EXPECT_EQ(results.size(), keys.size());// still can get the key
    val = MetaServiceClient::Get(results[0]);
    EXPECT_EQ(results[0], keys[0]);
    EXPECT_TRUE(val.empty()); // but value is nothing

    // clean
    MetaServiceClient::RemoveKeyFromSet("test_set", keys[0]);
    results = MetaServiceClient::GetKeysInSet("test_set");
    EXPECT_TRUE(results.empty());
}

TEST_F(MetaServiceClientTest, ZSetBasicOperations) {
    const std::string zsetKey = generate_test_key("zset_basic");
    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "member1"));
    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "member2"));

    MetaServiceClient::ZWrite(zsetKey, "member1", 10.5);
    EXPECT_DOUBLE_EQ(std::stod(MetaServiceClient::ZReadScore(zsetKey, "member1")), 10.5);

    auto results = MetaServiceClient::ZRead(zsetKey, 0);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].first, "member1");
    EXPECT_DOUBLE_EQ(results[0].second, 10.5);

    MetaServiceClient::ZWrite(zsetKey, "member2", 20.0);
    MetaServiceClient::ZWrite(zsetKey, "member3", 5.5);
    results = MetaServiceClient::ZRead(zsetKey, 0); // get all is from small score to large
    ASSERT_EQ(results.size(), 3);

    EXPECT_EQ(results[2].first, "member2");
    EXPECT_DOUBLE_EQ(results[2].second, 20.0);
    EXPECT_EQ(results[1].first, "member1");
    EXPECT_DOUBLE_EQ(results[1].second, 10.5);
    EXPECT_EQ(results[0].first, "member3");
    EXPECT_DOUBLE_EQ(results[0].second, 5.5);

    results = MetaServiceClient::ZRead(zsetKey, 2);
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0].first, "member2");
    EXPECT_EQ(results[1].first, "member1");

    MetaServiceClient::ZWrite(zsetKey, "member1", 25.0);
    results = MetaServiceClient::ZRead(zsetKey, 0);
    ASSERT_GE(results.size(), 1);
    EXPECT_DOUBLE_EQ(std::stod(MetaServiceClient::ZReadScore(zsetKey, "member1")), 25.0);

    EXPECT_EQ(results[2].first, "member1");

    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "member1"));
    EXPECT_EQ(MetaServiceClient::ZReadScore(zsetKey, "member1"), "");

    results = MetaServiceClient::ZRead(zsetKey, 0);
    ASSERT_EQ(results.size(), 2);

    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "member2"));
    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "member3"));
    results = MetaServiceClient::ZRead(zsetKey, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(MetaServiceClientTest, ZSetEdgeCases) {
    const std::string zsetKey = generate_test_key("zset_edge");

    // Test 1: test key not exist
    EXPECT_EQ(MetaServiceClient::ZReadScore(zsetKey, "non_exist"), "");

    // Test 2: test members has same score
    MetaServiceClient::ZWrite(zsetKey, "memberB", 15.0);
    MetaServiceClient::ZWrite(zsetKey, "memberA", 15.0);

    auto results = MetaServiceClient::ZRead(zsetKey, 0);
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0].first, "memberA");
    EXPECT_EQ(results[1].first, "memberB");

    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "memberA"));
    ASSERT_NO_THROW(MetaServiceClient::SingleZDelete(zsetKey, "memberB"));
}

TEST_F(MetaServiceClientTest, BatchZWrite) {
    std::string k1 = generate_test_key("zset1");
    std::string k2 = generate_test_key("zset2");
    std::vector<std::string> keys = {
        k1,
        k2,
        k2,
        k2
    };
    std::vector<std::string> members = {"m1", "m2", "m22", "m23"};
    std::vector<double> scores = {10.5, 20.0, 30.0, 25.0};

    ASSERT_NO_THROW(MetaServiceClient::BatchZWrite(keys, members, scores));

    auto res1 = MetaServiceClient::ZRead(keys[0], 0);
    ASSERT_EQ(res1.size(), 1);
    EXPECT_DOUBLE_EQ(res1[0].second, 10.5);

    auto res2 = MetaServiceClient::ZRead(keys[1], 0); // return scores from small->large
    ASSERT_EQ(res2.size(), 3);
    EXPECT_EQ(res2[0].first, "m2");
    EXPECT_EQ(res2[1].first, "m23");
    EXPECT_EQ(res2[2].first, "m22");

    std::vector<std::string> keysRead = {k1, k2};
    auto res3 = MetaServiceClient::BatchZRead(keysRead);
    ASSERT_EQ(res3.size(), 2);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}