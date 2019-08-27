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
#include "util.hpp"

#include <hta/hta.hpp>
#include <hta/ostream.hpp>

#include "../../include/hta/hta.hpp"
#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>

#include <cassert>

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

void throttle_copy(hta::Metric& src, hta::Metric& dst, hta::Duration chunk_interval)
{
    auto total_range = src.range();
    size_t processed = 0;
    auto count = src.count();
    hta::TimePoint previous_time;
    for (auto t0 = total_range.first; t0 <= total_range.second; t0 += chunk_interval)
    {
        print_progress(static_cast<double>((t0 - total_range.first).count()) /
                       (total_range.second - total_range.first).count());
        auto data = src.retrieve(t0, t0 + chunk_interval, { hta::Scope::closed, hta::Scope::open });
        for (auto tv : data)
        {
            processed++;
            if (processed % 100000000 == 0 || processed == count)
            {
                std::cout << processed << " / " << count << std::endl;
            }
            if (tv.time <= previous_time)
            {
                std::cout << "Non-Monotonic time " << tv.time << " at index " << (processed - 1)
                          << std::endl;
                continue;
            }
            previous_time = tv.time;
            dst.insert(tv);
        }
    }
    print_progress(1);
}

int main(int argc, char* argv[])
{
    assert(argc == 4);
    (void)argc;
    std::string config_file = argv[1];
    std::string src_name = argv[2];
    std::string dst_name = argv[3];

    auto config = read_json_from_file(std::filesystem::path(config_file));
    hta::Directory directory(config);

    std::cout.imbue(std::locale(""));
    throttle_copy(directory[src_name], directory[dst_name],
                  hta::duration_cast(std::chrono::hours(1024)));
}
