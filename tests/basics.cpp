#include <directory.hpp>
#include <metric.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

#include <my_filesystem.hpp>

#include <iostream>
using json = nlohmann::json;

TEST_CASE("HTA file can basically be written and read.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_basics.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    json config;
    config["type"] = "file";
    config["path"] = test_pwd;

    auto config_path = test_pwd / "config.json";
    std::ofstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(config_path);
    config_file << config;
    config_file.close();

    hta::TimeValue test_sample(hta::TimePoint(hta::Duration(23)), 42.0);
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        metric->insert(test_sample);
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        auto result =
            metric->retrieve(hta::TimePoint(hta::Duration(22)), hta::TimePoint(hta::Duration(24)));
        REQUIRE(result.size() == 1);
        REQUIRE(result.at(0).time == test_sample.time);
        REQUIRE(result.at(0).aggregate.sum == test_sample.value);
        REQUIRE(result.at(0).aggregate.minimum == test_sample.value);
        REQUIRE(result.at(0).aggregate.maximum == test_sample.value);
        REQUIRE(result.at(0).aggregate.count == 1);
        REQUIRE(result.at(0).aggregate.integral == 0);
        REQUIRE(result.at(0).aggregate.active_time == hta::Duration(0));
    }
}
