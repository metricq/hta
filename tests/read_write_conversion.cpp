// Copyright (c) 2019, ZIH,
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

TEST_CASE("HTA metrics are totally typesafe.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_conversion.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    // "Create read metric for later."
    {
        json config = { { "type", "file" },
                        { "path", test_pwd },
                        {
                            "metrics",
                            {
                                {
                                    { "name", "test.read" },
                                    { "mode", "W" },
                                    { "interval_min", 14400000000000 },
                                    { "interval_max", 1440000000000000 },
                                    { "interval_factor", 10 },
                                },
                            },
                        } };

        auto config_path = test_pwd / "config.json";
        std::ofstream config_file;
        config_file.exceptions(std::ios::badbit | std::ios::failbit);
        config_file.open(config_path);
        config_file << config;
        config_file.close();

        hta::Directory dir(config_path);
        // auto m = dir.write_metric("test.read");
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "test.read" / "raw.hta") > 0);

    // "Test read, write, and read_write metrics."
    {
        json config = { { "type", "file" },
                        { "path", test_pwd },
                        {
                            "metrics",
                            {
                                {
                                    { "name", "test.read" },
                                    { "mode", "R" },
                                    { "interval_min", 14400000000000 },
                                    { "interval_max", 1440000000000000 },
                                    { "interval_factor", 10 },
                                },
                                {
                                    { "name", "test.write" },
                                    { "mode", "W" },
                                    { "interval_min", 14400000000000 },
                                    { "interval_max", 1440000000000000 },
                                    { "interval_factor", 10 },
                                },
                                {
                                    { "name", "test.read_write" },
                                    { "mode", "RW" },
                                    { "interval_min", 14400000000000 },
                                    { "interval_max", 1440000000000000 },
                                    { "interval_factor", 10 },
                                },
                            },
                        } };

        auto config_path = test_pwd / "config.json";
        std::ofstream config_file;
        config_file.exceptions(std::ios::badbit | std::ios::failbit);
        config_file.open(config_path);
        config_file << config;
        config_file.close();

        hta::Directory dir(config_path);
        auto rw_as_rw = dir["test.read_write"];
        auto rw_as_r = dir.read_metric("test.read_write");
        auto rw_as_w = dir.write_metric("test.read_write");
        REQUIRE(rw_as_rw == rw_as_r);
        REQUIRE(rw_as_rw == rw_as_w);

        auto r_as_r = dir.read_metric("test.read");
        REQUIRE_THROWS_AS(dir["test.read"], hta::Exception);
        REQUIRE_THROWS_AS(dir.write_metric("test.read"), hta::Exception);

        auto w_as_w = dir.write_metric("test.write");
        REQUIRE_THROWS_AS(dir["test.write"], hta::Exception);
        REQUIRE_THROWS_AS(dir.read_metric("test.write"), hta::Exception);
    }
}