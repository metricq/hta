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

TEST_CASE("HTA metrics are totally typesafe.", "[hta]")
{
    // Unfortunately there are no portable unique temporary directory creation mechanisms
    // Let's just do it in the build folder
    // Also we don't clean up if a test fails - so we can inspect what the problem seems to be
    auto test_pwd = std::filesystem::current_path() / "hta_read_write.tmp";
    // Remove leftovers from past runs
    std::filesystem::remove_all(test_pwd);
    auto created = std::filesystem::create_directories(test_pwd);
    REQUIRE(created);

    // "Create read metric for later."
    {
        json config = {
            { "type", "file" },
            { "path", test_pwd },
            { "metrics",
              {
                  { "test.read",
                    {
                        { "mode", "W" },
                    } },
              } },
        };

        auto config_path = test_pwd / "config.json";
        std::ofstream config_file;
        config_file.exceptions(std::ios::badbit | std::ios::failbit);
        config_file.open(config_path);
        config_file << config;
        config_file.close();

        hta::Directory dir(config_path);
    }

    REQUIRE(std::filesystem::file_size(test_pwd / "test.read" / "raw.hta") > 0);

    // "Test read, write, and read_write metrics."
    {
        json config = {
            { "type", "file" },
            { "path", test_pwd },
            { "metrics",
              {
                  { "test.read",
                    {
                        { "mode", "R" },
                    } },
                  { "test.write",
                    {
                        { "mode", "W" },
                    } },
                  { "test.read_write",
                    {
                        { "mode", "RW" },
                    } },
              } },
        };

        auto config_path = test_pwd / "config.json";
        std::ofstream config_file;
        config_file.exceptions(std::ios::badbit | std::ios::failbit);
        config_file.open(config_path);
        config_file << config;
        config_file.close();

        hta::Directory dir(config_path);
        hta::TimeValue test_sample(hta::TimePoint(hta::Duration(23)), 42.0);

        auto& rw = dir["test.read_write"];
        rw.insert(test_sample);
        rw.retrieve(hta::TimePoint(hta::Duration(22)), hta::TimePoint(hta::Duration(24)), 100);

        auto& r = dir["test.read"];
        REQUIRE_THROWS_AS(r.insert(test_sample), hta::Exception);
        auto result =
            r.retrieve(hta::TimePoint(hta::Duration(22)), hta::TimePoint(hta::Duration(24)), 100);
        REQUIRE(result.size() == 0);

        auto& w = dir["test.write"];
        w.insert(test_sample);
        REQUIRE_THROWS_AS(
            w.retrieve(hta::TimePoint(hta::Duration(22)), hta::TimePoint(hta::Duration(24)), 100),
            hta::Exception);
    }
}