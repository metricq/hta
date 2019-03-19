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

TEST_CASE("Metric count interface works", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_count.tmp";
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

        SECTION("Invalid inputs")
        {
            SECTION("when begin is largen than end")
            {
                REQUIRE_THROWS(metric.count(tp(10s), tp(1s)));
            }

            SECTION("when begin equals end")
            {
                CHECK(metric.count(tp(1s), tp(1s)) == 1);
                CHECK(metric.count(tp(11s), tp(11s)) == 1);
                CHECK(metric.count(tp(85s), tp(85s)) == 1);
                CHECK(metric.count(tp(225s), tp(225s)) == 1);
                CHECK(metric.count(tp(250s), tp(250s)) == 0);
            }
        }

        SECTION("larger intervals are handled correctly")
        {
            SECTION("when used without parameters")
            {
                CHECK(metric.count() == 110);
            }

            SECTION("when the interval is between the first and the last raw value")
            {
                CHECK(metric.count(tp(20s), tp(220s)) == 109);

                // clang-format off
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::open, hta::Scope::open }) == 108);
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::open, hta::Scope::closed }) == 108);
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::open, hta::Scope::extended }) == 109);

                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::closed, hta::Scope::open }) == 108);
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::closed, hta::Scope::closed }) ==108);
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::closed, hta::Scope::extended }) == 109);

                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::extended, hta::Scope::open }) == 109);
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::extended, hta::Scope::closed }) == 109);
                CHECK(metric.count(tp(20s), tp(220s), { hta::Scope::extended, hta::Scope::extended }) == 110);
                // clang-format on
            }

            SECTION("when the interval is exactly from the first to the last timestamp")
            {
                CHECK(metric.count(tp(11s), tp(225s)) == 110);

                // clang-format off
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::open, hta::Scope::open }) == 108);
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::open, hta::Scope::closed }) == 109);
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::open, hta::Scope::extended }) == 109);

                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::closed, hta::Scope::open }) == 109);
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::closed, hta::Scope::closed }) ==110);
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::closed, hta::Scope::extended }) == 110);

                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::extended, hta::Scope::open }) == 109);
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::extended, hta::Scope::closed }) == 110);
                CHECK(metric.count(tp(11s), tp(225s), { hta::Scope::extended, hta::Scope::extended }) == 110);
                // clang-format on
            }

            SECTION("when the interval starts before the first timestamp and ends after the last "
                    "timestamp")
            {
                CHECK(metric.count(tp(1s), tp(230s)) == 110);
            }
        }

        SECTION("Tiny intervals are handled correctly")
        {
            SECTION("when containing one raw value")
            {
                CHECK(metric.count(tp(20s), tp(30s)) == 2);
            }

            SECTION("when the interval is excatly from one raw value to the next raw value")
            {
                SECTION("Case A")
                {
                    CHECK(metric.count(tp(11s), tp(21s)) == 2);

                    // clang-format off
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::open, hta::Scope::open }) == 0);
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::open, hta::Scope::closed }) == 1);
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::open, hta::Scope::extended }) == 1);

                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::closed, hta::Scope::open }) == 1);
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::closed, hta::Scope::closed }) == 2);
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::closed, hta::Scope::extended }) == 2);

                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::extended, hta::Scope::open }) == 1);
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::extended, hta::Scope::closed }) == 2);
                    CHECK(metric.count(tp(11s), tp(21s), { hta::Scope::extended, hta::Scope::extended }) == 2);
                }

                SECTION("Case B")
                {
                    CHECK(metric.count(tp(21s), tp(42s)) == 2);

                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::open, hta::Scope::open }) == 0);
                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::open, hta::Scope::closed }) == 1);
                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::open, hta::Scope::extended }) == 1);

                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::closed, hta::Scope::open }) == 1);
                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::closed, hta::Scope::closed }) == 2);
                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::closed, hta::Scope::extended }) == 2);

                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::extended, hta::Scope::open }) == 1);
                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::extended, hta::Scope::closed }) == 2);
                    CHECK(metric.count(tp(21s), tp(42s), { hta::Scope::extended, hta::Scope::extended }) == 2);
                    //clang-format on
                }
            }

            SECTION("when the interval is stricly between two raw values")
            {
                CHECK(metric.count(tp(12s), tp(20s)) == 1);

                //clang-format off
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::open, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::open, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::open, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::closed, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::closed, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::closed, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::extended, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::extended, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(12s), tp(20s), { hta::Scope::extended, hta::Scope::extended }) == 2);
                //clang-format on
            }

            SECTION("when the interval is the first row")
            {
                auto [epoch, _] = metric.range();

                CHECK(epoch == tp(11s));

                auto begin = interval_begin(epoch, 10s);
                auto end = interval_end(epoch, 10s);

                CHECK(begin == tp(10s));
                CHECK(end == tp(20s));

                //clang-format off
                CHECK(metric.count(begin, end, { hta::Scope::open, hta::Scope::open }) == 1);
                CHECK(metric.count(begin, end, { hta::Scope::open, hta::Scope::closed }) == 1);
                CHECK(metric.count(begin, end, { hta::Scope::open, hta::Scope::extended }) == 2);

                CHECK(metric.count(begin, end, { hta::Scope::closed, hta::Scope::open }) == 1);
                CHECK(metric.count(begin, end, { hta::Scope::closed, hta::Scope::closed }) == 1);
                CHECK(metric.count(begin, end, { hta::Scope::closed, hta::Scope::extended }) == 2);

                CHECK(metric.count(begin, end, { hta::Scope::extended, hta::Scope::open }) == 1);
                CHECK(metric.count(begin, end, { hta::Scope::extended, hta::Scope::closed }) == 1);
                CHECK(metric.count(begin, end, { hta::Scope::extended, hta::Scope::extended }) == 2);
                //clang-format on
            }

            SECTION("when the interval is the row before the first row")
            {
                auto [epoch, _] = metric.range();

                CHECK(epoch == tp(11s));

                auto begin = interval_begin(epoch, 10s) - 10s;
                auto end = interval_end(epoch, 10s) - 10s;

                CHECK(begin == tp(0s));
                CHECK(end == tp(10s));

                //clang-format off
                CHECK(metric.count(begin, end, { hta::Scope::open, hta::Scope::open }) == 0);
                CHECK(metric.count(begin, end, { hta::Scope::open, hta::Scope::closed }) == 0);
                CHECK(metric.count(begin, end, { hta::Scope::open, hta::Scope::extended }) == 1);

                CHECK(metric.count(begin, end, { hta::Scope::closed, hta::Scope::open }) == 0);
                CHECK(metric.count(begin, end, { hta::Scope::closed, hta::Scope::closed }) == 0);
                CHECK(metric.count(begin, end, { hta::Scope::closed, hta::Scope::extended }) == 1);

                CHECK(metric.count(begin, end, { hta::Scope::extended, hta::Scope::open }) == 0);
                CHECK(metric.count(begin, end, { hta::Scope::extended, hta::Scope::closed }) == 0);
                CHECK(metric.count(begin, end, { hta::Scope::extended, hta::Scope::extended }) == 1);
                //clang-format on
            }
        }

        SECTION("Edge-cases are handled properly")
        {
            SECTION("when it begins after the last timestamp")
            {
                //clang-format off
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::open, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::open, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::open, hta::Scope::extended }) == 0);

                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::closed, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::closed, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::closed, hta::Scope::extended }) == 0);

                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::extended, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::extended, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(501s), tp(504s), { hta::Scope::extended, hta::Scope::extended }) == 1);
                //clang-format on
            }

            SECTION("when it begins right at the last timestamp")
            {
                //clang-format off
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::open, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::open, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::open, hta::Scope::extended }) == 0);

                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::closed, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::closed, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::closed, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::extended, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::extended, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(225s), tp(504s), { hta::Scope::extended, hta::Scope::extended }) == 1);
                //clang-format on
            }

            SECTION("when it begins right before the last timestamp")
            {
                auto begin = tp(225s - hta::Duration(1));

                //clang-format off
                CHECK(metric.count(begin, tp(504s), { hta::Scope::open, hta::Scope::open }) == 1);
                CHECK(metric.count(begin, tp(504s), { hta::Scope::open, hta::Scope::closed }) == 1);
                CHECK(metric.count(begin, tp(504s), { hta::Scope::open, hta::Scope::extended }) == 1);

                CHECK(metric.count(begin, tp(504s), { hta::Scope::closed, hta::Scope::open }) == 1);
                CHECK(metric.count(begin, tp(504s), { hta::Scope::closed, hta::Scope::closed }) == 1);
                CHECK(metric.count(begin, tp(504s), { hta::Scope::closed, hta::Scope::extended }) == 1);

                CHECK(metric.count(begin, tp(504s), { hta::Scope::extended, hta::Scope::open }) == 2);
                CHECK(metric.count(begin, tp(504s), { hta::Scope::extended, hta::Scope::closed }) == 2);
                CHECK(metric.count(begin, tp(504s), { hta::Scope::extended, hta::Scope::extended }) == 2);
                //clang-format on

            }

            SECTION("when it ends before the first timestamp")
            {
                //clang-format off
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::open, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::open, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::open, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::closed, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::closed, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::closed, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::extended, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::extended, hta::Scope::closed }) == 0);
                CHECK(metric.count(tp(1s), tp(10s), { hta::Scope::extended, hta::Scope::extended }) == 1);
                //clang-format on
            }

            SECTION("when it ends right on the first timestamp")
            {
                //clang-format off
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::open, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::open, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::open, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::closed, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::closed, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::closed, hta::Scope::extended }) == 1);

                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::extended, hta::Scope::open }) == 0);
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::extended, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(1s), tp(11s), { hta::Scope::extended, hta::Scope::extended }) == 1);
                //clang-format on
            }

            SECTION("when it ends right after the first timestamp")
            {
                auto end  = tp(11s + hta::Duration(1));

                //clang-format off
                CHECK(metric.count(tp(1s), end, { hta::Scope::open, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(1s), end, { hta::Scope::open, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(1s), end, { hta::Scope::open, hta::Scope::extended }) == 2);

                CHECK(metric.count(tp(1s), end, { hta::Scope::closed, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(1s), end, { hta::Scope::closed, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(1s), end, { hta::Scope::closed, hta::Scope::extended }) == 2);

                CHECK(metric.count(tp(1s), end, { hta::Scope::extended, hta::Scope::open }) == 1);
                CHECK(metric.count(tp(1s), end, { hta::Scope::extended, hta::Scope::closed }) == 1);
                CHECK(metric.count(tp(1s), end, { hta::Scope::extended, hta::Scope::extended }) == 2);
                //clang-format on
            }
        }
    }
}
