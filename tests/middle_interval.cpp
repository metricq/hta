#include <hta/directory.hpp>
#include <hta/filesystem.hpp>
#include <hta/metric.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>
using json = nlohmann::json;

using namespace std::literals::chrono_literals;

constexpr auto offset = 1520012636139086277ns;
constexpr auto delta = 20000ns;

template <typename T>
constexpr hta::TimePoint tp(T duration)
{
    return hta::TimePoint(hta::duration_cast(duration + offset));
}

constexpr hta::TimeValue sample(int64_t i, double value)
{
    return { tp(delta * i), value };
}

TEST_CASE("HTA file can basically be written and read.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_middle_interval.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    json config = { { "type", "file" },
                    { "path", test_pwd },
                    {
                        "metrics",
                        {
                            {
                                { "name", "foo" },
                                { "mode", "RW" },
                                { "interval_min", 1000000 },
                                { "interval_factor", 10 },
                            },
                        },
                    } };

    {
        hta::Directory dir(config);
        auto metric = dir["foo"];

        for (int i = 0; i < 1000000; i++)
        {
            metric->insert(sample(i, i / 3.0));
        }
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config);
        auto metric = dir["foo"];

        // TODO check raw values
        {
            auto result = metric->retrieve(tp(0s), tp(10000s), hta::duration_cast(1000000ns));
        }
        {
            auto result = metric->retrieve(tp(0s), tp(10000s), hta::duration_cast(10000000ns));
        }
        {
            auto result = metric->retrieve(tp(0s), tp(10000s), hta::duration_cast(100000000ns));
        }
    }
}
