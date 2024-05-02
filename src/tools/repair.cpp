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

#include <hta/chrono.hpp>
#include <hta/hta.hpp>
#include <hta/ostream.hpp>

#include "../storage/file/metric.hpp"

#include <nitro/lang/string.hpp>
#include <nitro/options/arguments.hpp>
#include <nitro/options/parser.hpp>

#include <exception>
#include <filesystem>
#include <iostream>
#include <locale>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

struct Interval
{

    Interval(const std::string& from, const std::string& to)
    : from(hta::Duration(std::stoll(from))), to(hta::Duration(std::stoll(to)))
    {
    }

    bool contains(hta::TimePoint t) const
    {
        return from <= t && t <= to;
    }

    hta::TimePoint from;
    hta::TimePoint to;
};

bool intervals_contain(const std::vector<Interval>& intervals, hta::TimePoint t)
{
    for (const auto& interval : intervals)
    {
        if (interval.contains(t))
            return true;
    }

    return false;
}

void throttle_copy(hta::Metric& src, hta::Metric& dst, hta::Duration chunk_interval,
                   bool transform_absolute, std::optional<double> drop_below,
                   std::optional<double> drop_above, const std::vector<Interval>& drop_intervals)
{
    auto total_range = src.range();
    size_t processed = 0;
    hta::TimePoint previous_time;
    for (auto t0 = total_range.first; t0 <= total_range.second; t0 += chunk_interval)
    {
        print_progress(static_cast<double>((t0 - total_range.first).count()) /
                       (total_range.second - total_range.first).count());
        auto data = src.retrieve(t0, t0 + chunk_interval, { hta::Scope::closed, hta::Scope::open });
        for (auto tv : data)
        {
            processed++;
            if (tv.time <= previous_time)
            {
                std::cout << "Non-Monotonic time " << tv.time << " at index " << (processed - 1)
                          << std::endl;
                continue;
            }
            if (std::isnan(tv.value))
            {
                std::cout << "Dropping NaN at index " << (processed - 1) << std::endl;
                continue;
            }
            if (std::isinf(tv.value))
            {
                std::cout << "Dropping infinity at index " << (processed - 1) << std::endl;
                continue;
            }
            if ((drop_above && tv.value > *drop_above) || (drop_below && tv.value < *drop_below))
            {
                std::cout << "Dropping clamped value (" << tv.value << ") at index "
                          << (processed - 1) << std::endl;
                continue;
            }
            if (intervals_contain(drop_intervals, tv.time))
            {
                std::cout << "Dropping timepoint" << tv.time.time_since_epoch().count()
                          << " at index " << (processed - 1) << std::endl;
                continue;
            }

            if (transform_absolute && tv.value < 0)
            {
                std::cout << "Use absolute of negative value at index " << (processed - 1) << "\n";
                tv.value = -tv.value;
            }

            previous_time = tv.time;
            dst.insert(tv);
        }
    }
    print_progress(1);
}

int main(int argc, char* argv[])
{
    nitro::options::parser parser(argv[0], "a tool to repair hta metrics");

    // accept the metric name as positional argument
    parser.accept_positionals(1);
    parser.positional_metavar("metric");

    parser.toggle("abs", "replace all negative values with their absolute value");
    parser.toggle("help", "show usage").short_name("h");

    parser.option("drop-above", "drop all data points with a value larger than the given value")
        .optional();
    parser.option("drop-below", "drop all data points with a value lesser than the given value")
        .optional();

    parser
        .multi_option("drop-interval", "drops all datapoints that are in the given closed interval")
        .metavar("{FROM_TS}-{TO_TS}")
        .optional();

    nitro::options::arguments options;

    try
    {
        options = parser.parse(argc, argv);
    }
    catch (std::exception& e)
    {
        std::cerr << "Failed to parse arguments: " << e.what() << std::endl;
        parser.usage();

        return 1;
    }

    if (options.given("help"))
    {
        parser.usage();
        return 0;
    }

    if (options.positionals().size() != 1)
    {
        std::cerr << "Exactly one metric is required." << std::endl;
        parser.usage();

        return 1;
    }

    std::vector<Interval> drop_intervals;

    try
    {
        for (auto& line : options.get_all("drop-interval"))
        {
            auto splitted = nitro::lang::split(line, "-");
            if (splitted.size() != 2)
            {
                throw std::runtime_error("cannot parse format: " + line);
            }

            drop_intervals.emplace_back(splitted[0], splitted[1]);
        }
    }
    catch (std::exception& e)
    {
        std::cerr << "Failed to parse drop-interval: " << e.what() << std::endl;
        parser.usage();
        return 1;
    }

    std::optional<double> drop_above;
    std::optional<double> drop_below;

    if (options.provided("drop-above"))
    {
        drop_above = std::stof(options.get("drop-above"));
    }

    if (options.provided("drop-below"))
    {
        drop_below = std::stof(options.get("drop-below"));
    }

    auto src_folder = std::filesystem::path(options.positionals()[0]);

    bool use_absolute = options.given("abs");

    if (!std::filesystem::exists(src_folder))
    {
        std::cerr << "The given input hta metric doesn't exists: " << src_folder << std::endl;

        return 1;
    }

    auto src_backup_folder = src_folder;
    src_backup_folder +=
        ".backup-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());

    // as the backup is tagged, this should practically not happen
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
    auto src_storage = std::make_unique<hta::storage::file::Metric>(
        hta::storage::file::FileOpenTag::Read(), src_backup_folder);
    auto dst_storage = std::make_unique<hta::storage::file::Metric>(
        hta::storage::file::FileOpenTag::Write(), src_folder, src_storage->meta());

    hta::Metric src(std::move(src_storage));
    hta::Metric dst(std::move(dst_storage));

    std::cout.imbue(std::locale(""));
    throttle_copy(src, dst, hta::duration_cast(std::chrono::hours(1024)), use_absolute, drop_below,
                  drop_above, drop_intervals);

    std::cout << std::endl;
}
