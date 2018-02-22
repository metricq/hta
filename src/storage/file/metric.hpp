#pragma once

#include "file.hpp"

#include "../metric.hpp"

#include <hta/chrono.hpp>
#include <hta/exception.hpp>
#include <hta/my_filesystem.hpp>
#include <hta/types.hpp>

#include <chrono>
#include <map>

namespace hta::storage::file
{
class Metric : public storage::Metric
{
private:
    /*
     * Currently we write a bunch of stuff to the header, but don't actual support deviations.
     * Later versions of the code may support different time duration periods
     */
    struct Header
    {
        Header() = default; // default C'tor for reading

        Header(Duration interval)
        : version{ 1 }, interval{ interval.count() }, duration_period{ Duration::period::num,
                                                                       Duration::period::den }
        {
        }

        void check(Duration interval)
        {
            if (version != 1)
                throw_exception("Unsupported HTA file format version ", version, ", supported 1");
            if (duration_period.num != Duration::period::num ||
                duration_period.den != Duration::period::den)
                throw_exception("Unsupported HTA chrono duration period.");
            if (this->interval != interval.count())
                throw_exception("Mismatching intervals in HTA file expected: ", interval.count(),
                                ", actual: ", this->interval);
        }

        uint64_t version;
        int64_t interval;
        struct
        {
            uint64_t num;
            uint64_t den;
        } duration_period;

        // Only add stuff - don't change it
    };
    using RawFile = File<Header, TimeValue>;
    using HtaFile = File<Header, TimeAggregate>;

public:
    Metric(const std::filesystem::path& path);

    void insert(TimeValue tv) override;
    void insert(Row row) override;

    void flush() override;

    std::vector<TimeValue> get(TimePoint begin, TimePoint ebd, IntervalScope scope) override;
    std::vector<TimeAggregate> get(TimePoint t0, TimePoint t1, Duration interval,
                                   IntervalScope scope) override;

    TimeValue last() override;
    TimeAggregate last(Duration interval) override;
    std::pair<TimePoint, TimePoint> range() override;

    std::size_t size() override;
    std::size_t size(Duration interval) override;

private:
    std::filesystem::path path_hta(Duration interval);
    std::filesystem::path path_raw();

    uint64_t find_raw_time_index_before_or_on(TimePoint t, uint64_t left, uint64_t right);
    uint64_t find_raw_time_index_on_or_after(TimePoint t, uint64_t left, uint64_t right);

    TimePoint get_raw_ts(uint64_t index);

    TimePoint epoch();
    TimePoint epoch(Duration interval);

    RawFile& file_raw()
    {
        return file_raw_;
    }

    HtaFile& file_hta(Duration interval);

    std::filesystem::path path_;
    RawFile file_raw_;
    // TODO use better data structure to optimize accesses for lowest interval
    // e.g. linear search through sorted vector
    std::map<Duration, HtaFile> files_hta_;
};
}
