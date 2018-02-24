#include <hta/directory.hpp>
#include <hta/metric.hpp>
#include <hta/my_filesystem.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>
using json = nlohmann::json;

using namespace std::literals::chrono_literals;

constexpr auto offset = 1500000000s;
constexpr hta::TimePoint tp(int64_t duration_seconds)
{
    return hta::TimePoint(hta::duration_cast(offset + std::chrono::seconds(duration_seconds)));
}

TEST_CASE("HTA file can basically be written and read.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_scope.tmp";
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

    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];

        for (int i = 0; i < 1000000; i++)
        {
            metric->insert({ tp(i), double(i) });
        }
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];

        {
            hta::IntervalScope is = { hta::Scope::closed, hta::Scope::open };
            {
                auto result = metric->retrieve(tp(10100), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 2);
            }
            {
                auto result = metric->retrieve(tp(10099), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 2);
            }
            {
                auto result = metric->retrieve(tp(10100), tp(10301), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
        }
        {
            hta::IntervalScope is = { hta::Scope::closed, hta::Scope::closed };
            {
                auto result = metric->retrieve(tp(10100), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
            {
                auto result = metric->retrieve(tp(10099), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
            {
                auto result = metric->retrieve(tp(10100), tp(10301), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
        }
        {
            hta::IntervalScope is = { hta::Scope::closed, hta::Scope::extended };
            {
                auto result = metric->retrieve(tp(100), tp(300), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
            {
                auto result = metric->retrieve(tp(99), tp(300), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
            {
                auto result = metric->retrieve(tp(100), tp(301), hta::duration_cast(100s), is);
                CHECK(result.size() == 4);
            }
        }
        {
            hta::IntervalScope is = { hta::Scope::open, hta::Scope::open };
            {
                auto result = metric->retrieve(tp(10100), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 1);
            }
            {
                auto result = metric->retrieve(tp(10099), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 2);
            }
            {
                auto result = metric->retrieve(tp(10100), tp(10301), hta::duration_cast(100s), is);
                CHECK(result.size() == 2);
            }
        }
        {
            hta::IntervalScope is = { hta::Scope::extended, hta::Scope::open };
            {
                auto result = metric->retrieve(tp(10100), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 2);
            }
            {
                auto result = metric->retrieve(tp(10099), tp(10300), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
            {
                auto result = metric->retrieve(tp(10100), tp(10301), hta::duration_cast(100s), is);
                CHECK(result.size() == 3);
            }
        }
    }
}
