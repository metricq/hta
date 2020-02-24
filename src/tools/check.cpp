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
#include "../storage/file/file.hpp"
#include "../storage/file/metric.hpp"

#include <hta/chrono.hpp>
#include <hta/ostream.hpp>

#include <boost/program_options.hpp>

#include <iostream>
#include <string>

#include <cmath>

constexpr size_t chunk_size = 4096;

// Heuristic values
constexpr hta::Value value_min = -1e20;
constexpr hta::Value value_max = 1e20;

constexpr auto genesis =
    hta::TimePoint(hta::Duration(946684800000000000ll)); // No metricq before 01.01.2000
auto future = hta::Clock::now();

namespace hta::storage::file
{
void check_raw(File<Metric::Header, TimeValue>& file, bool fast = false)
{
    auto size = file.size();
    TimePoint previous_time;
    std::vector<TimeValue> data(chunk_size);

    size_t issue_count = 0;
    size_t chunk_begin = 0;
    if (fast && size > chunk_size)
    {
        chunk_begin = size - chunk_size;
    }
    for (; chunk_begin < size; chunk_begin += chunk_size)
    {
        data.resize(std::min(chunk_size, size - chunk_begin));
        file.read(data, chunk_begin);
        for (size_t index = 0; index < data.size(); index++)
        {
            auto tv = data[index];
            auto absolute_index = chunk_begin + index;

            if (!std::isfinite(tv.value))
            {
                std::cerr << "Warning: [" << absolute_index << "] Non-finite value " << tv.value
                          << " at " << tv.time << std::endl;
                issue_count++;
            }
            else if (tv.value < value_min)
            {
                std::cerr << "Warning: [" << absolute_index << "] Implausible value " << tv.value
                          << " at " << tv.time << std::endl;
                issue_count++;
            }
            else if (tv.value > value_max)
            {
                std::cerr << "Warning: [" << absolute_index << "] Implausible value: " << tv.value
                          << " at " << tv.time << std::endl;
                issue_count++;
            }

            if (tv.time < previous_time)
            {
                std::cerr << "Error: [" << absolute_index << "] non-monotonous time " << tv.time
                          << " follows " << previous_time << std::endl;
                issue_count++;
            }
            else if (tv.time == previous_time)
            {
                std::cerr << "Error: [" << absolute_index << "] duplicate time " << tv.time
                          << std::endl;
                issue_count++;
            }
            else
            {
                previous_time = tv.time;
            }
        }
    }
    std::cout << "Finished check of " << size << " entries: " << issue_count << " issues."
              << std::endl;
}

void check_hta(File<Metric::Header, TimeAggregate>& file, Duration interval, TimePoint raw_epoch,
               TimePoint raw_end, bool fast = false)
{
    auto size = file.size();
    auto epoch = interval_begin(raw_epoch, interval);

    TimePoint previous_time;
    std::vector<TimeAggregate> data;
    data.reserve(chunk_size);

    size_t issue_count = 0;

    // check last chunk consistency
    data.resize(1);
    file.read(data, size - 1);
    if (data[0].time + interval != interval_begin(raw_end, interval))
    {
        std::cerr << "Error: Invalid last interval timestamp: " << data[0].time
                  << ", expected: " << interval_begin(raw_end, interval) << ", raw_end: " << raw_end
                  << ", interval: " << interval.count() << std::endl;
        issue_count++;
    }

    size_t chunk_begin = 0;
    if (fast && size > chunk_size)
    {
        chunk_begin = size - chunk_size;
    }
    for (; chunk_begin < size; chunk_begin += chunk_size)
    {
        data.resize(std::min(chunk_size, size - chunk_begin));
        file.read(data, chunk_begin);
        for (size_t sub_index = 0; sub_index < data.size(); sub_index++)
        {
            auto ta = data[sub_index];
            auto index = chunk_begin + sub_index;

            auto expected_time = epoch + interval * index;
            if (expected_time != ta.time)
            {
                std::cerr << "Error: [" << index << "] bogus time, expected: " << expected_time
                          << ", actual: " << ta.time << std::endl;
                issue_count++;
            }
            if (ta.time < previous_time)
            {
                std::cerr << "Error: [" << index << "] non-monotonous time " << ta.time
                          << " follows " << previous_time << std::endl;
                issue_count++;
            }
            else if (ta.time == previous_time)
            {
                std::cerr << "Error: [" << index << "] duplicate time " << ta.time << std::endl;
                issue_count++;
            }
            else
            {
                previous_time = ta.time;
            }

            if (!std::isfinite(ta.aggregate.integral) || !std::isfinite(ta.aggregate.maximum) ||
                !std::isfinite(ta.aggregate.minimum) || !std::isfinite(ta.aggregate.sum))
            {
                std::cerr << "Warning: [" << index << "] non-finite aggregate " << ta.aggregate
                          << " at " << ta.time << std::endl;
                issue_count++;
            }
        }
    }
    std::cout << "Finished check of " << size << " entries: " << issue_count << " issues."
              << std::endl;
}

void check(const std::filesystem::path dir, bool fast = false)
{
    std::cout << "[" << dir << "] Checking metric directory " << (fast ? " fast" : " full")
              << std::endl;

    auto raw_path = dir / std::filesystem::path("raw.hta");
    std::cout << "[" << raw_path << "] Checking raw file" << std::endl;
    File<Metric::Header, TimeValue> raw_file{ FileOpenTag::Read(), raw_path };

    if (raw_file.size() == 0)
    {
        std::cerr << "[" << raw_path << "] Error: empty raw file, aborting check." << std::endl;
        return;
    }
    auto raw_epoch = raw_file.read(0).time;
    auto raw_end = raw_file.read(raw_file.size() - 1).time;

    if (raw_epoch > raw_end)
    {
        std::cerr << "[" << raw_path << "] Error: First after last raw timestamp: " << raw_epoch
                  << ", " << raw_end << std::endl;
    }

    if (raw_epoch < genesis || raw_epoch > future)
    {
        std::cerr << "[" << raw_path << "] Error: Implausible first raw timestamp: " << raw_epoch
                  << std::endl;
    }

    if (raw_end < genesis || raw_end > future)
    {
        std::cerr << "[" << raw_path << "] Error: Implausible last raw timestamp: " << raw_end
                  << std::endl;
    }

    auto header = raw_file.header();
    check_raw(raw_file, fast);

    for (auto interval = Duration(header.interval_min); interval.count() <= header.interval_max;
         interval *= header.interval_factor)
    {

        auto hta_path = dir / (std::to_string(interval.count()) + ".hta");
        std::cout << "[" << hta_path << "] Checking HTA file" << std::endl;
        File<Metric::Header, TimeAggregate> hta_file{ FileOpenTag::Read(), hta_path };
        if (interval.count() != hta_file.header().interval)
        {
            std::cerr << "[" << hta_path << "] Error: "
                      << " mismatching interval: name: " << interval
                      << ", header: " << hta_file.header().interval << std::endl;
        }

        if (interval_begin(raw_epoch, interval) == interval_begin(raw_end, interval))
        {
            if (hta_file.size() != 0)
            {
                std::cerr << "Top HTA file should be empty but it is not." << std::endl;
            }
            break;
        }

        // TODO Check interval factor/min
        check_hta(hta_file, interval, raw_epoch, raw_end, fast);
    }
}
} // namespace hta::storage::file

namespace po = boost::program_options;

int main(int argc, char* argv[])
{
    bool fast = false;
    std::vector<std::string> directories;
    po::options_description desc("Check HTA databases for consistency");
    desc.add_options()("fast", po::bool_switch(&fast), "check only the most recent data")(
        "directory", po::value<std::vector<std::string>>(&directories));
    po::positional_options_description p;
    p.add("directory", -1);
    try
    {
        po::parsed_options parsed =
            po::command_line_parser(argc, argv).options(desc).positional(p).run();
        po::variables_map vm;
        po::store(parsed, vm);
        po::notify(vm);
    }
    catch (const std::exception& e)
    {
        std::cerr << "options error: " << e.what() << std::endl;
        return -1;
    }
    for (const auto& directory : directories)
    {
        try
        {
            hta::storage::file::check(directory, fast);
        }
        catch (const std::exception& e)
        {
            std::cerr << "[" << directory << "] Error: exception thrown: " << e.what() << std::endl;
        }
    }
}
