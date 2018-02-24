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
constexpr hta::TimePoint tp(int64_t duration_seconds, int64_t epsilon = 0)
{
    return hta::TimePoint(hta::Duration(epsilon) +
                          hta::duration_cast(offset + std::chrono::seconds(duration_seconds)));
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

        // RAW tests
        {
            auto count_200 = [&metric](int64_t begin_epsilon, int64_t end_epsilon,
                                       hta::IntervalScope interval_scope) {
                return metric
                    ->retrieve(tp(10100, begin_epsilon), tp(10300, end_epsilon), interval_scope)
                    .size();
            };
            {
                hta::IntervalScope is = { hta::Scope::closed, hta::Scope::open };
                CHECK(count_200(0, 0, is) == 200);
                CHECK(count_200(-1, 0, is) == 200);
                CHECK(count_200(1, 0, is) == 199);
                CHECK(count_200(0, -1, is) == 200);
                CHECK(count_200(0, 1, is) == 201);
            }
            {
                hta::IntervalScope is = { hta::Scope::closed, hta::Scope::closed };
                CHECK(count_200(0, 0, is) == 201);
                CHECK(count_200(-1, 0, is) == 201);
                CHECK(count_200(1, 0, is) == 200);
                CHECK(count_200(0, -1, is) == 200);
                CHECK(count_200(0, 1, is) == 201);
            }
            {
                hta::IntervalScope is = { hta::Scope::closed, hta::Scope::extended };
                CHECK(count_200(0, 0, is) == 201);
                CHECK(count_200(-1, 0, is) == 201);
                CHECK(count_200(1, 0, is) == 200);
                CHECK(count_200(0, -1, is) == 201);
                CHECK(count_200(0, 1, is) == 202);
            }
            {
                hta::IntervalScope is = { hta::Scope::open, hta::Scope::open };
                CHECK(count_200(0, 0, is) == 199);
                CHECK(count_200(-1, 0, is) == 200);
                CHECK(count_200(1, 0, is) == 199);
                CHECK(count_200(0, -1, is) == 199);
                CHECK(count_200(0, 1, is) == 200);
            }
            {
                hta::IntervalScope is = { hta::Scope::extended, hta::Scope::open };
                CHECK(count_200(0, 0, is) == 200);
                CHECK(count_200(-1, 0, is) == 201);
                CHECK(count_200(1, 0, is) == 200);
                CHECK(count_200(0, -1, is) == 200);
                CHECK(count_200(0, 1, is) == 201);
            }
            CHECK(count_200(0, 0, { hta::Scope::infinity, hta::Scope ::infinity }) == 1000000);
        }

        // Aggregate tests
        {
            auto count_2 = [&metric](int64_t begin_epsilon, int64_t end_epsilon,
                                     hta::IntervalScope interval_scope) {
                return metric
                    ->retrieve(tp(10100, begin_epsilon), tp(10300, end_epsilon),
                               hta::duration_cast(100s), interval_scope)
                    .size();
            };
            {
                hta::IntervalScope is = { hta::Scope::closed, hta::Scope::open };
                CHECK(count_2(0, 0, is) == 2);
                CHECK(count_2(-1, 0, is) == 2);
                CHECK(count_2(1, 0, is) == 1);
                CHECK(count_2(0, -1, is) == 2);
                CHECK(count_2(0, 1, is) == 3);
            }
            {
                hta::IntervalScope is = { hta::Scope::closed, hta::Scope::closed };
                CHECK(count_2(0, 0, is) == 3);
                CHECK(count_2(-1, 0, is) == 3);
                CHECK(count_2(1, 0, is) == 2);
                CHECK(count_2(0, -1, is) == 2);
                CHECK(count_2(0, 1, is) == 3);
            }
            {
                hta::IntervalScope is = { hta::Scope::closed, hta::Scope::extended };
                CHECK(count_2(0, 0, is) == 3);
                CHECK(count_2(-1, 0, is) == 3);
                CHECK(count_2(1, 0, is) == 2);
                CHECK(count_2(0, -1, is) == 3);
                CHECK(count_2(0, 1, is) == 4);
            }
            {
                hta::IntervalScope is = { hta::Scope::open, hta::Scope::open };
                CHECK(count_2(0, 0, is) == 1);
                CHECK(count_2(-1, 0, is) == 2);
                CHECK(count_2(1, 0, is) == 1);
                CHECK(count_2(0, -1, is) == 1);
                CHECK(count_2(0, 1, is) == 2);
            }
            {
                hta::IntervalScope is = { hta::Scope::extended, hta::Scope::open };
                CHECK(count_2(0, 0, is) == 2);
                CHECK(count_2(-1, 0, is) == 3);
                CHECK(count_2(1, 0, is) == 2);
                CHECK(count_2(0, -1, is) == 2);
                CHECK(count_2(0, 1, is) == 3);
            }
        }
    }
}
