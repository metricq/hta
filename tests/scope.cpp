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

    json config = { { "type", "file" },
                    { "path", test_pwd },
                    {
                      "metrics",
                              {
                                      {
                                              { "name", "foo" },
                                      },
                              },
                    } };

    auto config_path = test_pwd / "config.json";
    std::ofstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(config_path);
    config_file << config;
    config_file.close();

    constexpr auto count = 1000000;
    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];

        for (int i = 0; i < count; i++)
        {
            metric->insert({ tp(i), double(i) });
        }
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "foo" / "raw.hta") > 0);

    {
        hta::Directory dir(config_path);
        auto metric = dir["foo"];
        CHECK(metric->range().first == tp(0));
        CHECK(metric->range().second == tp(count - 1));
        // Checking out of file timestamps
        {
            // closed-closed on corner timestamps
            auto result =
                metric->retrieve(tp(0), tp(count - 1), { hta::Scope::closed, hta::Scope::closed });
            CHECK(result.size() == count);
            CHECK(result.at(0).time == tp(0));
            CHECK(result.at(count - 1).time == tp(count - 1));
        }
        {
            // open-open on corner timestamps
            auto result =
                metric->retrieve(tp(0), tp(count - 1), { hta::Scope::open, hta::Scope::open });
            CHECK(result.size() == count - 2);
            CHECK(result.at(0).time == tp(1));
            CHECK(result.at(count - 3).time == tp(count - 2));
        }
        {
            // closed-closed out range
            auto result = metric->retrieve(tp(0, -1), tp(count - 1, 1),
                                           { hta::Scope::closed, hta::Scope::closed });
            CHECK(result.size() == count);
            CHECK(result.at(0).time == tp(0));
            CHECK(result.at(count - 1).time == tp(count - 1));
        }
        {
            // open-open out range
            auto result = metric->retrieve(tp(0, -1), tp(count - 1, 1),
                                           { hta::Scope::open, hta::Scope::open });
            CHECK(result.size() == count);
            CHECK(result.at(0).time == tp(0));
            CHECK(result.at(count - 1).time == tp(count - 1));
        }
        {
            auto result =
                metric->retrieve(tp(0), tp(count - 1), { hta::Scope ::closed, hta::Scope::open });
            CHECK(result.size() == count - 1);
            CHECK(result.at(0).time == tp(0));
            CHECK(result.at(count - 2).time == tp(count - 2));
        }

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
