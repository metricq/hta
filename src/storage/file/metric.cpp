#include "metric.hpp"

#include <my_filesystem.hpp>
#include <types.hpp>

namespace hta::storage::file
{

Metric::Metric(const std::filesystem::path& path)
: path_(path), file_raw_(FileOpenTag::ReadWrite(), path_raw(), Header(Duration(0)))
{
}

void Metric::insert(TimeValue tv)
{
    file_raw().write(tv);
}

void Metric::insert(Row row)
{
    auto& file = file_hta(row.interval);
    TimeAggregate aggregate{ row.time, row.aggregate };
    file.write(aggregate);
}

void Metric::flush()
{
    file_raw().flush();
    for (auto& elem : files_hta_)
    {
        elem.second.flush();
    }
}

uint64_t Metric::find_raw_time_index(TimePoint t, uint64_t left, uint64_t right)
{
    assert(left < right);
    if (right - left == 1)
    {
        return left;
    }
    uint64_t pivot = (left + right) / 2;
    TimePoint t_pivot = get_raw_ts(pivot);
    if (t >= t_pivot)
    {
        return find_raw_time_index(t, pivot, right);
    }
    else
    {
        return find_raw_time_index(t, left, pivot);
    }
}

uint64_t Metric::find_raw_time_index(TimePoint t)
{
    return find_raw_time_index(t, 0, size());
}

TimePoint Metric::get_raw_ts(uint64_t index)
{
    TimePoint time;
    file_raw().read(time, index * sizeof(TimeValue));
    return time;
}

uint64_t Metric::size()
{
    auto fsize = file_raw().size();
    assert(0 == fsize % sizeof(TimeValue));
    return fsize / sizeof(TimeValue);
}

uint64_t Metric::size(Duration interval)
{
    auto fsize = file_hta(interval).size();
    assert(0 == fsize % sizeof(Aggregate));
    return fsize / sizeof(Aggregate);
}

std::vector<TimeValue> Metric::get(TimePoint t0, TimePoint t1, IntervalScope scope)
{
    uint64_t i0 = find_raw_time_index(t0);
    uint64_t i1 = find_raw_time_index(t1);
    assert(scope == IntervalScope::CLOSED_EXTENDED);

    i1++; // extended
    size_t count = i1 - i0;
    std::vector<TimeValue> result(count);
    file_raw().read(result, i0 * sizeof(TimeValue));
    return result;
}

TimePoint Metric::epoch()
{
    // TODO cache!
    TimePoint start;
    file_raw().read(start, 0);
    return start;
}

TimePoint Metric::epoch(Duration interval)
{
    return interval_begin(epoch(), interval);
}

std::vector<TimeAggregate> Metric::get(TimePoint t0, TimePoint t1, Duration interval,
                                       IntervalScope scope)
{
    if (size() == 0)
    {
        return {};
    }

    auto& file = file_hta(interval);

    assert(t1 >= t0);
    auto start = epoch(interval);
    auto d0 = t0 - start;
    if (d0.count() < 0)
    {
        d0 = Duration(0);
    }
    auto d1 = t1 - start;

    uint64_t i0 = d0 / interval;
    uint64_t i1 = d1 / interval + 1; // @TODO extended?!

    i1 = std::min(size(), i1);
    size_t count = i1 - i0;

    std::vector<TimeAggregate> result(count);
    file.read(result, i0 * sizeof(TimeAggregate));

#ifndef NDEBUG
    // Check consistency of times
    TimePoint t = start + i0 * interval;
    for (const auto& ta : result)
    {
        assert(ta.time == t);
        t += interval;
    }
#endif
    return result;
}

std::pair<TimePoint, TimePoint> Metric::range()
{
    TimeValue first, last;
    file_raw().read(first, 0);
    file_raw().read(last, size() - 1);
    return { first.time, last.time };
}

std::filesystem::path Metric::path_hta(Duration interval)
{
    std::stringstream ss;
    ss << std::chrono::duration_cast<std::chrono::nanoseconds>(interval).count() << ".hta";
    return path_ / std::filesystem::path(ss.str());
}

std::filesystem::path Metric::path_raw()
{
    return path_ / std::filesystem::path("raw.hta");
}

Metric::MetricFile& Metric::file_hta(Duration interval)
{
    auto it = files_hta_.find(interval);
    if (it == files_hta_.end())
    {
        bool added;
        std::tie(it, added) = files_hta_.try_emplace(interval, FileOpenTag::ReadWrite(),
                                                     path_hta(interval), Header(interval));
        assert(added);
    }
    return it->second;
}
}
