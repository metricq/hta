// Copyright (c) 2018, ZIH,
// Technische Universitaet Dresden,
// Federal Republic of Germany
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of metricq nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#include <hta/directory.hpp>
#include <hta/filesystem.hpp>
#include <hta/metric.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

#include <chrono>
#include <iostream>

using namespace std::literals::chrono_literals;

constexpr auto swap_point = 1440000000000000000ns;
constexpr auto delta = 20000ns;
constexpr auto step = 1440000000000000ns;

TEST_CASE("Ensure no overflow happens with specific timestamps.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_overflow.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    json config = {
        { "type", "file" },
        { "path", test_pwd },
        { "metrics",
          {
              { "foo",
                {
                    { "mode", "RW" },
                    { "interval_min", 14400000000000 },
                    { "interval_max", 1440000000000000 },
                    { "interval_factor", 10 },
                } },
          } },
    };

    auto config_path = test_pwd / "config.json";
    std::ofstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(config_path);
    config_file << config;
    config_file.close();

    {
        hta::Directory dir(config_path);
        auto& metric = dir["foo"];

        metric.insert(hta::TimeValue(hta::TimePoint(hta::duration_cast(swap_point - delta)), 42.0));
        metric.insert(hta::TimeValue(hta::TimePoint(hta::duration_cast(swap_point + delta)), 43.0));
        // We need this to trigger writing another interval high up
        metric.insert(
            hta::TimeValue(hta::TimePoint(hta::duration_cast(swap_point + step + delta)), 44.0));
    }
    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);
    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "14400000000000.hta") > 0);
    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "144000000000000.hta") > 0);
    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "1440000000000000.hta") > 0);
    REQUIRE(!std::filesystem::exists(test_pwd / "foo" / "14400000000000000.hta"));
    {
        hta::Directory dir(config);
        auto& metric = dir["foo"];

        // TODO check raw values
        hta::TimePoint t_min(hta::duration_cast(swap_point - delta));
        hta::TimePoint t_max(hta::duration_cast(swap_point + delta));

        {
            // raw values
            auto result = metric.retrieve(t_min, t_max, hta::duration_cast(1000000ns),
                                          { hta::Scope::extended, hta::Scope::closed });
            REQUIRE(result.size() == 2);
        }
        auto max_interval = hta::duration_cast(1440000000000000ns);
        {
            auto result = metric.retrieve(t_min, t_max, max_interval,
                                          { hta::Scope::extended, hta::Scope::closed });
            REQUIRE(result.size() == 2);
            REQUIRE(result.at(0).interval == max_interval);
            REQUIRE(result.at(1).interval == max_interval);
        }
        {
            auto result = metric.retrieve(t_min, t_max, max_interval * 11,
                                          { hta::Scope::extended, hta::Scope::closed });
            REQUIRE(result.size() == 2);
            REQUIRE(result.at(0).interval == max_interval);
            REQUIRE(result.at(1).interval == max_interval);
        }
    }
}
