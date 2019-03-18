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
#include <hta/hta.hpp>

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>

#include <cstdint>
#include <cstdlib>

#include "pcg_random.hpp"

using json = nlohmann::json;

json read_json_from_file(const std::filesystem::path& path)
{
    std::ifstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(path);
    json config;
    config_file >> config;
    return config;
}

int main(int argc, char* argv[])
{
    std::string config_file = "config.json";
    if (argc > 1)
    {
        config_file = argv[1];
    }
    auto config = read_json_from_file(std::filesystem::path(config_file));

    std::string metric_prefix = "bench.s0.";

    std::ifstream file;
    file.exceptions(std::ifstream::badbit);
    std::string filename = config.at("value_file");
    file.open(filename);
    file.seekg(0, std::fstream::end);

    std::vector<double> fake_values;
    auto size_bytes = file.tellg();
    if (size_bytes == 0 || size_bytes % sizeof(hta::Value))
    {
        throw std::runtime_error("file size empty or not a multiple of sizeof metricq::Value");
    }

    fake_values.resize(size_bytes / sizeof(hta::Value));
    file.seekg(0);
    file.read(reinterpret_cast<char*>(fake_values.data()), size_bytes);

    pcg64 random;
    std::uniform_int_distribution<size_t> distribution(0, fake_values.size() - 1);

    hta::Directory out_directory(config);

    // 2018-01-01T00:00:00+00:00
    hta::TimePoint time_base(hta::Duration(1514764800ll * 1000000000));
    auto duration = std::chrono::hours(24 * 365);
    auto interval = std::chrono::milliseconds(1);

    for (int i = 0; i < 6; i++)
    {
        auto fake_value_iter = fake_values.begin() + distribution(random);
        auto& metric = out_directory[metric_prefix + std::to_string(i)];
        for (std::chrono::nanoseconds time_off {0}; time_off < duration; time_off += interval)
        {
            metric.insert(hta::TimeValue(time_base + time_off, *fake_value_iter));
            if (++fake_value_iter == fake_values.end())
            {
                fake_value_iter = fake_values.begin();
            }
        }
    }
}
