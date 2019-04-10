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

#include <cassert>

namespace po = boost::program_options;

namespace hta::storage::file
{
template <class T>
void dump_summary(const std::filesystem::path& path)
{
    File<Metric::Header, T> file{ FileOpenTag::Read(), path };
    if (file.size() > 0)
    {
        std::cout << "first timestamp: " << file.read(0).time << "\n";
        std::cout << "last timestamp " << file.read(file.size() - 1).time << "\n";
    }
    std::cout << file.size() << " entries\n";
}

void dump_header(const std::filesystem::path& path)
{
    File<Metric::Header, TimePoint> file{ FileOpenTag::Read(), path };
    auto header = file.header();
    std::cout << "HTA header of " << path << "\n";
    std::cout << "version: " << header.version << "\n";
    std::cout << "interval: " << header.interval << "\n";
    std::cout << "duration_period: " << header.duration_period.num << " / "
              << header.duration_period.den << "\n";
    std::cout << "interval_min: " << header.interval_min << "\n";
    std::cout << "interval_max: " << header.interval_max << "\n";
    std::cout << "interval_factor: " << header.interval_factor << "\n";
    if (header.interval == 0)
    {
        std::cout << "File contains raw TimeValue\n";
        dump_summary<TimeValue>(path);
    }
    else
    {
        std::cout << "File contains TimeAggregate\n";
        dump_summary<TimeAggregate>(path);
    }
}
} // namespace hta::storage::file

int main(int argc, char* argv[])
{
    std::cout << "Size of a TimeValue: " << sizeof(hta::TimePoint) << "\n";
    std::cout << "Size of a TimeAggregate: " << sizeof(hta::TimeAggregate) << "\n";
    if (argc != 2)
    {
        return -1;
    }
    hta::storage::file::dump_header(argv[1]);
}
