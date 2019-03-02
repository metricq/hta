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
#include <hta/metric.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

#include <hta/filesystem.hpp>

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

TEST_CASE("HTA file can basically be written and read.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_restore.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    json config = {
        { "type", "file" },
        { "path", test_pwd },
        { "metrics",
          {
              { "foo", {} },
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
        auto metric = dir["foo"];
    }
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        metric->insert(sample(11s, -37));
    }
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        metric->insert(sample(21s, -36));
        metric->insert(sample(42s, -30));
    }
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        metric->insert(sample(48s, -20));
        metric->insert(sample(53s, -10));
        metric->insert(sample(67s, 0));
    }
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        metric->insert(sample(80s, -10));
        for (auto i = 101s; i < 200s; i += 1s)
            metric->insert(sample(i, 20));
        metric->insert(sample(203s, 31));
        metric->insert(sample(217s, 35));
    }
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        metric->insert(sample(219s, 45));
        metric->insert(sample(225s, 35));
        // TODO insert points directly at level border and check if it works correctly
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];

        // TODO check raw values

        {
            auto result = metric->retrieve(tp(0s), tp(300s), 31);
            CHECK(result.size() == 110);
        }
        constexpr double nsticks = 1e9;
        {
            auto result = metric->retrieve(tp(0s), tp(300s), 30);
            REQUIRE(result.size() == 21);

            CHECK(result[0].time == tp(10s));
            CHECK(result[0].aggregate.active_time == 9s);
            CHECK(result[0].aggregate.count == 1);
            CHECK(result[0].aggregate.integral == 9 * nsticks * -36.);
            CHECK(result[0].aggregate.sum == -37.);
            CHECK(result[0].aggregate.maximum == -36.);
            CHECK(result[0].aggregate.minimum == -37.);

            CHECK(result[1].time == tp(20s));
            CHECK(result[1].aggregate.active_time == 10s);
            CHECK(result[1].aggregate.count == 1);
            CHECK(result[1].aggregate.integral == nsticks * (-36. + 9 * -30.));

            CHECK(result[2].time == tp(30s));
            CHECK(result[2].aggregate.active_time == 10s);
            CHECK(result[2].aggregate.count == 0);
            CHECK(result[2].aggregate.integral == nsticks * (10 * -30.));
            CHECK(result[2].aggregate.sum == 0.);
            CHECK(result[2].aggregate.maximum == -30);
            CHECK(result[2].aggregate.minimum == -30);

            CHECK(result[3].time == tp(40s));
            CHECK(result[3].aggregate.active_time == 10s);
            CHECK(result[3].aggregate.count == 2);
            CHECK(result[3].aggregate.integral == nsticks * (2 * -30. + 6 * -20. + 2 * -10.));
            CHECK(result[3].aggregate.sum == -30 - 20);
            CHECK(result[3].aggregate.maximum == -10);
            CHECK(result[3].aggregate.minimum == -30);

            CHECK(result[20].time == tp(210s));
            CHECK(result[20].aggregate.active_time == 10s);
            CHECK(result[20].aggregate.count == 2);
            CHECK(result[20].aggregate.integral == nsticks * (7 * 35. + 2 * 45. + 1 * 35.));
            CHECK(result[20].aggregate.sum == 45. + 35.);
            CHECK(result[20].aggregate.maximum == 45);
            CHECK(result[20].aggregate.minimum == 35);

            // TODO if we ever fix the "one pixel lost" thing, there will be another interval here
        }
        {
            auto result = metric->retrieve(tp(0s), tp(300s), 5);
            REQUIRE(result.size() == 21);
        }
        {
            auto result = metric->retrieve(tp(0s), tp(300s), 3);
            REQUIRE(result.size() == 2);

            CHECK(result[0].time == tp(0s));
            CHECK(result[0].aggregate.active_time == 89s);
            CHECK(result[0].aggregate.count == 7);
            double integral = 0;
            integral += 10 * -36.;
            integral += 21 * -30;
            integral += 6 * -20.;
            integral += 5 * -10;
            integral += 14 * 0;
            integral += 13 * -10;
            integral += 20 * 20;
            CHECK(result[0].aggregate.integral == integral * nsticks);
            CHECK(result[0].aggregate.sum == -37 - 36 - 30 - 20 - 10 + 0 - 10);
            CHECK(result[0].aggregate.maximum == 20.);
            CHECK(result[0].aggregate.minimum == -37.);

            CHECK(result[1].time == tp(100s));
            CHECK(result[1].aggregate.active_time == 100s);
            CHECK(result[1].aggregate.count == 99);
            CHECK(result[1].aggregate.integral == (99 * 20. + 1 * 31.) * nsticks);
            CHECK(result[1].aggregate.sum == 99 * 20.);
            CHECK(result[1].aggregate.maximum == 31.);
            CHECK(result[1].aggregate.minimum == 20.);
        }
    }
}
