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

uint64_t Metric::find_raw_time_index_before_or_on(TimePoint t, uint64_t left, uint64_t right)
{
    // left < right means right != 0
    // assumes monotonicity ts(left) < ts(right)
    // invariant ts(left) <= t < ts(right)
    // right can be size() // out of range, but the pivot can never be
    assert(left < right);
    if (right - left == 1)
    {
        return left;
    }
    uint64_t pivot = (left + right) / 2;
    TimePoint t_pivot = get_raw_ts(pivot);
    if (t >= t_pivot)
    {
        return find_raw_time_index_before_or_on(t, pivot, right);
    }
    else
    {
        return find_raw_time_index_before_or_on(t, left, pivot);
    }
}

uint64_t Metric::find_raw_time_index_on_or_after(TimePoint t, uint64_t left, uint64_t right)
{
    // left < right means right != 0
    // assumes monotonicity ts(left) < ts(right)
    // invariant ts(left) < t <= ts(right)
    // right can be size() // out of range, but the pivot can never be
    assert(left < right);
    if (right - left == 1)
    {
        return right;
    }
    uint64_t pivot = (left + right) / 2;
    TimePoint t_pivot = get_raw_ts(pivot);
    if (t > t_pivot)
    {
        return find_raw_time_index_on_or_after(t, pivot, right);
    }
    else
    {
        return find_raw_time_index_on_or_after(t, left, pivot);
    }
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
    assert(0 == fsize % sizeof(TimeAggregate));
    return fsize / sizeof(TimeAggregate);
}

std::vector<TimeValue> Metric::get(TimePoint begin, TimePoint end, IntervalScope scope)
{
    const auto sz = size();
    if (sz == 0)
    {
        return {};
    }
    uint64_t index_begin;
    /* this is the index of the end _element_ (not after!) according to scope.end
     * It may be out of scope (==size()) in some cases */
    uint64_t index_end;
    // This is quite complex and possibly bad for performance
    // TODO optimize // simplify
    switch (scope.begin)
    {
    case Scope::closed:
        index_begin = find_raw_time_index_before_or_on(begin, 0, sz);
        if (index_begin < sz)
        {
            index_begin++;
        }
        break;
    case Scope::open:
        index_begin = find_raw_time_index_before_or_on(begin, 0, sz);
        break;
    case Scope::extended:
        index_begin = find_raw_time_index_before_or_on(begin, 0, sz);
        break;
    }

    switch (scope.end)
    {
    case Scope::closed:
        index_end = find_raw_time_index_before_or_on(end, 0, sz);
        break;
    case Scope::open:
        index_end = find_raw_time_index_on_or_after(end, 0, sz);
        if (index_end > 0)
        {
            index_end--;
        }
        break;
    case Scope::extended:
        index_end = find_raw_time_index_on_or_after(end, 0, sz);
    }

    size_t count = index_end - index_begin;
    if (index_end < sz)
    {
        // We include also the requested end element, but only if it's not outside the range
        count++;
    }
    std::vector<TimeValue> result(count);
    file_raw().read(result, index_begin * sizeof(TimeValue));
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

TimeValue Metric::last()
{
    TimeValue tv;
    file_raw().read(tv, (size() - 1) * sizeof(TimeValue));
    return tv;
}

TimeAggregate Metric::last(Duration interval)
{
    TimeAggregate ta;
    file_hta(interval).read(ta, (size(interval) - 1) * sizeof(TimeAggregate));
    return ta;
}

std::pair<TimePoint, TimePoint> Metric::range()
{
    TimeValue first;
    file_raw().read(first, 0);
    return { first.time, last().time };
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
