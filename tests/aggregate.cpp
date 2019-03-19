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

// This test checks the aggregate() methods of a hta::Metric

TEST_CASE("Metric aggregate interface works", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_aggregate.tmp";
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
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config_path);
        auto& metric = dir["foo"];

        SECTION("larger intervals works")
        {
            SECTION("when the interval is between the first and the last raw value")
            {
                auto result = metric.aggregate(tp(20s), tp(220s));
                CHECK(result.count == 108);
                CHECK(result.minimum == -36);
                CHECK(result.maximum == 45);
                CHECK(result.sum == 1985);
                CHECK(result.mean_sum() == 1985 / 108.);
                auto integral = (-36 * 1) + (-30 * 21) + (-20 * 6) + (-10 * 5) + (0 * 14) +
                                (-10 * 13) + (20 * 119) + (31 * 4) + (35 * 14) + (45 * 2) +
                                (35 * 1);
                CHECK(result.active_time == 200s);
                CHECK(result.integral == integral * hta::duration_cast(1s).count());
            }

            SECTION("when the interval is exactly from the first to the last timestamp")
            {
                auto result = metric.aggregate(tp(11s), tp(225s));
                CHECK(result.count == 109);
                CHECK(result.minimum == -36);
                CHECK(result.maximum == 45);
                CHECK(result.sum == 2010);
                CHECK(result.mean_sum() == 2010 / 109.);
                auto integral = (-36 * 10) + (-30 * 21) + (-20 * 6) + (-10 * 5) + (0 * 14) +
                                (-10 * 13) + (20 * 119) + (31 * 4) + (35 * 14) + (45 * 2) +
                                (35 * 6);
                CHECK(result.active_time == 214s);
                CHECK(result.integral == integral * hta::duration_cast(1s).count());
            }

            SECTION("when the interval starts before the first timestamp and ends after the last "
                    "timestamp")
            {
                auto result = metric.aggregate(tp(1s), tp(230s));
                CHECK(result.count == 110);
                CHECK(result.minimum == -37);
                CHECK(result.maximum == 45);
                CHECK(result.sum == 1973);
                auto integral = (-36 * 10) + (-30 * 21) + (-20 * 6) + (-10 * 5) + (0 * 14) +
                                (-10 * 13) + (20 * 119) + (31 * 4) + (35 * 14) + (45 * 2) +
                                (35 * 6);
                CHECK(result.active_time == 214s);
                CHECK(result.integral == integral * hta::duration_cast(1s).count());
            }
        }

        SECTION("Tiny intervals are handled correctly")
        {
            SECTION("when containing one raw value")
            {
                auto result = metric.aggregate(tp(20s), tp(30s));
                CHECK(result.count == 1);
                CHECK(result.minimum == -36);
                CHECK(result.maximum == -30);
                CHECK(result.mean_sum() == -36);
                CHECK(result.mean_integral() == (-36 - 9 * 30) / 10.0);
                CHECK(result.active_time == 10s);
            }

            SECTION("when the interval is excatly from one raw values to the next raw value")
            {
                SECTION("Case A")
                {
                    auto result = metric.aggregate(tp(11s), tp(21s));
                    CHECK(result.count == 1);
                    CHECK(result.minimum == -36);
                    CHECK(result.maximum == -36);
                    CHECK(result.mean_sum() == -36);
                    CHECK(result.mean_integral() == -36 / 10.0);
                    CHECK(result.active_time == 10s);
                }

                SECTION("Case B")
                {
                    auto result = metric.aggregate(tp(21s), tp(42s));
                    CHECK(result.count == 1); // the right guy isn't technically inside the interval
                                              // as a point
                    CHECK(result.sum == -36);
                    CHECK(result.minimum == -36);
                    CHECK(result.maximum == -30);
                    CHECK(result.mean_integral() == -30);
                    CHECK(result.active_time == 21s);
                }
            }

            SECTION("when the interval is stricly between two raw values")
            {
                auto result = metric.aggregate(tp(12s), tp(20s));
                CHECK(result.count == 0);
                CHECK(result.minimum == -36);
                CHECK(result.maximum == -36);
                CHECK(result.mean_sum() == -36);
                CHECK(result.mean_integral() == -36 / 8.0);
                CHECK(result.active_time == 8s);
            }
        }

        SECTION("Edge-cases are handled properly")
        {
            SECTION("when it begins after the last timestamp")
            {
                auto result = metric.aggregate(tp(501s), tp(504s));

                CHECK(result.active_time == 0s);
                CHECK(result.count == 0);
                CHECK(result.sum == 0);
                CHECK(result.minimum == std::numeric_limits<hta::Value>::infinity());
                CHECK(result.maximum == -std::numeric_limits<hta::Value>::infinity());
                CHECK(result.integral == 0);
            }

            SECTION("when it begins right at the last timestamp")
            {
                auto result = metric.aggregate(tp(225s), tp(504s));

                CHECK(result.active_time == 0s);
                CHECK(result.count == 0);
                CHECK(result.sum == 0);
                CHECK(result.minimum == std::numeric_limits<hta::Value>::infinity());
                CHECK(result.maximum == -std::numeric_limits<hta::Value>::infinity());
                CHECK(result.integral == 0);
            }

            SECTION("when it begins right before the last timestamp")
            {
                auto result = metric.aggregate(tp(225s - hta::Duration(1)), tp(504s));

                CHECK(result.active_time == hta::Duration(1));
                CHECK(result.count == 1);
                CHECK(result.sum == 35);
                CHECK(result.minimum == 35);
                CHECK(result.maximum == 35);
                CHECK(result.integral == 35);
            }

            SECTION("when it ends before the first timestamp")
            {
                auto result = metric.aggregate(tp(1s), tp(10s));

                CHECK(result.active_time == 0s);
                CHECK(result.count == 0);
                CHECK(result.sum == 0);
                CHECK(result.minimum == std::numeric_limits<hta::Value>::infinity());
                CHECK(result.maximum == -std::numeric_limits<hta::Value>::infinity());
                CHECK(result.integral == 0);
            }

            SECTION("when it ends right on the first timestamp")
            {
                auto result = metric.aggregate(tp(1s), tp(11s));

                CHECK(result.active_time == 0s);
                CHECK(result.count == 1);
                CHECK(result.sum == -37);
                CHECK(result.minimum == -37);
                CHECK(result.maximum == -37);
                CHECK(result.integral == 0);
            }

            SECTION("when it ends right after the first timestamp")
            {
                auto result = metric.aggregate(tp(1s), tp(11s + hta::Duration(1)));

                CHECK(result.active_time == hta::Duration(1));
                CHECK(result.count == 1);
                CHECK(result.sum == -37);
                CHECK(result.minimum == -37);
                CHECK(result.maximum == -37);
                CHECK(result.integral == -37);
            }
        }
    }
}
