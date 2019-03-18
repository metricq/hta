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
#include <hta/chrono.hpp>
#include <hta/directory.hpp>
#include <hta/metric.hpp>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#include <nlohmann/json.hpp>

#include <hta/filesystem.hpp>

#include <chrono>
#include <iostream>
using json = nlohmann::json;

TEST_CASE("HTA prefix configuration.", "[hta]")
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
        json config = {
            { "type", "file" },
            { "path", test_pwd },
            { "metrics",
              {
                  { "prefix",
                    {
                        { "prefix", true },
                        { "mode", "RW" },
                        { "interval_min", 13370000000000 },
                        { "interval_max", 1337000000000000 },
                        { "interval_factor", 20 },
                    } },
                  { "foo",
                    {
                        { "prefix", false },
                        { "mode", "RW" },
                    } },
                  { "bar",
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

        auto& m = dir["prefix.metric"];
        REQUIRE(m.meta().interval_min == hta::Duration(13370000000000));
        REQUIRE(m.meta().interval_max == hta::Duration(1337000000000000));
        REQUIRE(m.meta().interval_factor == 20);

        REQUIRE_THROWS_AS(dir["invalid.metric"], hta::Exception);

        REQUIRE_THROWS_AS(dir["foo.metric"], hta::Exception);
        REQUIRE_THROWS_AS(dir["bar.metric"], hta::Exception);

        dir["prefix."]; // technically allowed
        dir["prefix.other"];
        dir["foo"];
        dir["bar"];
    }
}