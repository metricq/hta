// Copyright (c) 2018-2020, ZIH,
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

#include "../storage/file/metric.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>

#include <cassert>

void throttle_copy(hta::storage::file::Metric::RawFile& src, hta::Metric& dst, size_t chunk_size)
{
    size_t processed = 0;
    hta::TimePoint previous_time;

    std::vector<hta::TimeValue> read_buffer(chunk_size);
    auto size = src.size();
    for(size_t read_index = 0; read_buffer.size() == chunk_size; read_index += chunk_size)
    {
        print_progress(static_cast<double>(read_index) / size);
        src.read(read_buffer, read_index);
        for (auto tv : read_buffer)
        {
            processed++;
            if (tv.time <= previous_time)
            {
                std::cout << "Non-Monotonic time " << tv.time << " at read_index " << (processed - 1)
                          << std::endl;
                continue;
            }
            if (std::isnan(tv.value))
            {
                std::cout << "Dropping NaN at read_index " << (processed - 1) << std::endl;
                continue;
            }
            if (std::isinf(tv.value))
            {
                std::cout << "Dropping infinity at read_index " << (processed - 1) << std::endl;
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
    if (argc != 2 || argv[1] == std::string("--help") || argv[1] == std::string("-h"))
    {
        std::cout << argv[0] << " - a tool to repair hta metrics" << std::endl;
        std::cout << "Usage: " << argv[0] << " path_to_metric_folder" << std::endl;

        return 0;
    }

    auto src_folder = std::filesystem::path(argv[1]);

    if (!std::filesystem::exists(src_folder))
    {
        std::cerr << "The given input hta metric doesn't exists: " << src_folder << std::endl;

        return 1;
    }

    auto src_backup_folder = src_folder;
    src_backup_folder +=
        ".backup-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());

    // as the backup is tagged, this should pratically not happen
    if (std::filesystem::exists(src_backup_folder))
    {
        std::cerr << "The backup folder of the given hta metric already exists: "
                  << src_backup_folder << std::endl;

        return 1;
    }

    // move file to backup folder
    std::filesystem::rename(src_folder, src_backup_folder);
    std::filesystem::create_directory(src_folder);

    // open hta metrics
    hta::storage::file::Metric::RawFile src_raw_file(
        hta::storage::file::FileOpenTag::Read(),
        src_backup_folder / std::filesystem::path("raw.hta")
    );
    auto meta = hta::Meta(
        hta::Duration(src_raw_file.header().interval_min),
        hta::Duration(src_raw_file.header().interval_max),
        src_raw_file.header().interval_factor
    );
    auto dst_storage = std::make_unique<hta::storage::file::Metric>(
        hta::storage::file::FileOpenTag::Write(), src_folder, meta);

    hta::Metric dst(std::move(dst_storage));

    std::cout.imbue(std::locale(""));
    throttle_copy(src_raw_file, dst, 1024*1024);

    std::cout << std::endl;
}
