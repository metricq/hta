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
#pragma once

#include "file.hpp"

#include "../metric.hpp"

#include <hta/chrono.hpp>
#include <hta/exception.hpp>
#include <hta/filesystem.hpp>
#include <hta/types.hpp>

#include <chrono>
#include <map>

namespace hta::storage::file
{
class Metric : public storage::Metric
{
public: // for the dump tool
    /*
     * Currently we write a bunch of stuff to the header, but don't actual support deviations.
     * Later versions of the code may support different time duration periods
     */
    struct Header
    {
        Header() = default; // default C'tor for reading

        Header(Duration interval, Meta meta)
        : version{ 2 }, interval{ interval.count() },
          duration_period{ Duration::period::num, Duration::period::den },
          interval_min{ meta.interval_min.count() }, interval_factor{ meta.interval_factor },
          interval_max{ meta.interval_max.count() }
        {
        }

        /**
         * Call this after reading it from a file
         */
        void restore(size_t read_bytes)
        {
            if (read_bytes != sizeof(Header))
            {
                throw_exception("Unexpected header size ", read_bytes, ", expected ",
                                sizeof(Header));
            }
            // For now we don't support different versions
            if (version != 2)
            {
                throw_exception("Unsupported HTA file format version ", version, ", supported 2");
            }
            if (duration_period.num != Duration::period::num ||
                duration_period.den != Duration::period::den)
            {
                throw_exception("Unsupported HTA chrono duration period.");
            }
        }

        uint64_t version;
        int64_t interval;
        struct
        {
            uint64_t num;
            uint64_t den;
        } duration_period;
        int64_t interval_min;
        int64_t interval_factor;
        int64_t interval_max;

        // Only add stuff - don't change it
    };
    using RawFile = File<Header, TimeValue>;
    using HtaFile = File<Header, TimeAggregate>;

public:
    Metric(FileOpenTag::Read file_open_tag, const std::filesystem::path& path)
    : open_mode_(file_open_tag.mode), path_(path), file_raw_(file_open_tag, path_raw())
    {
    }

    template <typename FOT>
    Metric(FOT file_open_tag, const std::filesystem::path& path, Meta meta)
    : open_mode_(file_open_tag.mode), path_(path),
      file_raw_(file_open_tag, path_raw(), Header(Duration(0), meta))
    {
    }

    Meta meta() const override;
    Mode mode() const override;

    void insert(TimeValue tv) override;
    void insert(Row row) override;

    void flush() override;

    std::vector<TimeValue> get(TimePoint begin, TimePoint ebd, IntervalScope scope) override;
    std::vector<TimeAggregate> get(TimePoint begin, TimePoint end, Duration interval,
                                   IntervalScope scope) override;

    size_t count(TimePoint begin, TimePoint end, IntervalScope scope) override;

    TimeValue last() override;
    TimeAggregate last(Duration interval) override;
    std::pair<TimePoint, TimePoint> range() override;

    std::size_t size() override;
    std::size_t size(Duration interval) override;

private:
    std::filesystem::path path_hta(Duration interval);
    std::filesystem::path path_raw();

    std::pair<uint64_t, uint64_t> find_index(TimePoint begin, TimePoint end, IntervalScope scope);

    int64_t find_index_before_or_on_binary(TimePoint t, int64_t left, int64_t right, int64_t sz);
    int64_t find_index_before_or_on(TimePoint t, int64_t sz);
    int64_t find_index_on_or_after_binary(TimePoint t, int64_t left, int64_t right, int64_t sz);
    int64_t find_index_on_or_after(TimePoint t, int64_t sz);

    TimePoint get_raw_ts(uint64_t index);

    TimePoint epoch();
    TimePoint epoch(Duration interval);

    RawFile& file_raw()
    {
        return file_raw_;
    }

    HtaFile& file_hta(Duration interval);

private:
    Mode open_mode_;
    std::filesystem::path path_;
    RawFile file_raw_;
    // TODO use better data structure to optimize accesses for lowest interval
    // e.g. linear search through sorted vector
    std::map<Duration, HtaFile> files_hta_;
};
} // namespace hta::storage::file
