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

#include <hta/ostream.hpp>

#include <boost/program_options.hpp>

#include <iostream>
#include <string>

#include <cmath>

// from https://stackoverflow.com/a/36315819/620382
#define PBSTR "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#define PBWIDTH 60

void print_progress(double percentage)
{
    int val = (int)(percentage * 100);
    int lpad = (int)(percentage * PBWIDTH);
    int rpad = PBWIDTH - lpad;
    printf("\r%3d%% [%.*s%*s]", val, lpad, PBSTR, rpad, "");
    fflush(stdout);
}

constexpr size_t chunk_size = 4096;

namespace hta::storage::file
{
void check_raw(File<Metric::Header, TimeValue>& file)
{
    auto size = file.size();
    TimePoint previous_time;
    std::vector<TimeValue> data(chunk_size);

    size_t issue_count = 0;
    for (size_t chunk_begin = 0; chunk_begin < size; chunk_begin += chunk_size)
    {
        print_progress((double)chunk_begin / size);
        data.resize(std::min(chunk_size, size - chunk_begin));
        file.read(data, chunk_begin);
        for (size_t index = 0; index < data.size(); index++)
        {
            auto tv = data[index];

            if (tv.time < previous_time)
            {
                std::cout << "WARNING [" << (chunk_begin + index) << "] non-monotonous time "
                          << tv.time << " follows " << previous_time << std::endl;
                issue_count++;
            }
            else if (tv.time == previous_time)
            {
                std::cout << "WARNING [" << (chunk_begin + index) << "] duplicate time " << tv.time
                          << std::endl;
                issue_count++;
            }
            else
            {
                previous_time = tv.time;
            }
            if (!std::isfinite(tv.value))
            {
                std::cout << "WARNING [" << (chunk_begin + index) << "] non-finite value "
                          << tv.value << " at " << tv.time << std::endl;
                issue_count++;
            }
        }
    }
    print_progress(1);
    std::cout << "Finished check of " << size << " entries: " << issue_count << " issues.";
}

void check_hta(File<Metric::Header, TimeAggregate>& file, Duration interval, TimePoint raw_epoch)
{
    auto size = file.size();
    auto epoch = interval_begin(raw_epoch, interval);

    TimePoint previous_time;
    std::vector<TimeAggregate> data(chunk_size);

    size_t issue_count = 0;
    for (size_t chunk_begin = 0; chunk_begin < size; chunk_begin += chunk_size)
    {
        print_progress((double)chunk_begin / size);
        data.resize(std::min(chunk_size, size - chunk_begin));
        file.read(data, chunk_begin);
        for (size_t sub_index = 0; sub_index < data.size(); sub_index++)
        {
            auto ta = data[sub_index];
            auto index = chunk_begin + sub_index;

            auto expected_time = epoch + interval * index;
            if (expected_time != ta.time)
            {
                std::cout << "WARNING [" << index << "] bogus time, expected: " << expected_time
                          << ", actual: " << ta.time << std::endl;
                issue_count++;
            }
            if (ta.time < previous_time)
            {
                std::cout << "WARNING [" << index << "] non-monotonous time " << ta.time
                          << " follows " << previous_time << std::endl;
                issue_count++;
            }
            else if (ta.time == previous_time)
            {
                std::cout << "WARNING [" << index << "] duplicate time " << ta.time << std::endl;
                issue_count++;
            }
            else
            {
                previous_time = ta.time;
            }
            if (!std::isfinite(ta.aggregate.integral) || !std::isfinite(ta.aggregate.maximum) ||
                !std::isfinite(ta.aggregate.minimum) || !std::isfinite(ta.aggregate.sum))
            {
                std::cout << "WARNING [" << index << "] non-finite aggregate " << ta.aggregate
                          << " at " << ta.time << std::endl;
                issue_count++;
            }
        }
    }
    print_progress(1);
    std::cout << "Finished check of " << size << " entries: " << issue_count << " issues."
              << std::endl;
}

void check(const std::filesystem::path& dir)
{
    std::cout << "checking raw file" << std::endl;
    File<Metric::Header, TimeValue> raw_file{ FileOpenTag::Read(),
                                              dir / std::filesystem::path("raw.hta") };
    if (raw_file.size() == 0)
    {
        std::cout << "empty raw file" << std::endl;
        return;
    }
    auto raw_epoch = raw_file.read(0).time;
    auto header = raw_file.header();
    check_raw(raw_file);
    for (auto interval = Duration(header.interval_min);; interval *= header.interval_factor)
    {
        std::filesystem::path filename = std::to_string(interval.count()) + ".hta";
        std::cout << "checking file: " << filename << std::endl;
        File<Metric::Header, TimeAggregate> hta_file{ FileOpenTag::Read(), dir / filename };
        if (interval.count() != hta_file.header().interval)
        {
            std::cout << "ERROR: " << filename << " mismatching interval: name: " << interval
                      << ", header: " << hta_file.header().interval;
        }
        // TODO Check interval factor/min
        check_hta(hta_file, interval, raw_epoch);
    }
}
} // namespace hta::storage::file

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "missing argument (path)\n";
        return -1;
    }
    hta::storage::file::check(argv[1]);
}
