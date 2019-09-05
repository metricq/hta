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

#include <chrono>
#include <iostream>
using json = nlohmann::json;

using namespace std::literals::chrono_literals;

constexpr auto offset = 1519130000s;

template <typename T>
constexpr hta::TimePoint tp(T duration)
{
    return hta::TimePoint(hta::duration_cast(duration + offset));
}

template <typename T>
constexpr hta::TimeValue sample(T duration, double value)
{
    return { tp(duration), value };
}

// This test checks the retrieve() methods of a hta::Metric

TEST_CASE("HTA file can basically be written and read.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_aggregation.tmp";
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
                    { "interval_min", hta::duration_cast(std::chrono::seconds(10)).count() },
                    { "interval_max", hta::duration_cast(std::chrono::seconds(1000)).count() },
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
        metric.insert(sample(11s, -37));
        metric.insert(sample(21s, -36));
        metric.insert(sample(42s, -30));
        metric.insert(sample(48s, -20));
        metric.insert(sample(53s, -10));
        metric.insert(sample(67s, 0));
        metric.insert(sample(80s, -10));
        for (auto i = 101s; i < 200s; i += 1s)
            metric.insert(sample(i, 20));
        metric.insert(sample(203s, 31));
        metric.insert(sample(217s, 35));
        metric.insert(sample(219s, 45));
        metric.insert(sample(225s, 35));
        // TODO insert points directly at level border and check if it works correctly
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config_path);
        auto& metric = dir["foo"];

        {
            auto flex =
                metric.retrieve_flex(tp(0s), tp(300s), hta::duration_cast(std::chrono::seconds(9)));
            CHECK(std::holds_alternative<std::vector<hta::TimeValue>>(flex));
            const auto& result = std::get<std::vector<hta::TimeValue>>(flex);
            CHECK(result.size() == 110);
        }
        {
            auto flex = metric.retrieve_flex(tp(0s), tp(300s),
                                             hta::duration_cast(std::chrono::seconds(10)));
            CHECK(std::holds_alternative<std::vector<hta::Row>>(flex));
            const auto& result = std::get<std::vector<hta::Row>>(flex);
            CHECK(result.size() == 21);
        }
    }
}
