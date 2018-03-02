#include <hta/directory.hpp>
#include <hta/filesystem.hpp>
#include <hta/metric.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

#include <iostream>
using json = nlohmann::json;

TEST_CASE("HTA meta fun.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_meta.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    json config = {
        { "type", "file" },
        { "path", test_pwd },
        {
            "metrics",
            {
                {
                    { "name", "foo" },
                    { "mode", "RW" },
                    { "interval_min", 1337000000 },
                    { "interval_factor", 42 },
                },
            },
        },
    };
    {
        hta::Directory dir(config);
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);
}
