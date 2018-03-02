#include "metric.hpp"

#include <hta/filesystem.hpp>
#include <hta/types.hpp>

namespace hta::storage::file
{
Meta Metric::meta() const
{
    return Meta{ Duration(file_raw_.header().interval_min), file_raw_.header().interval_factor };
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

uint64_t Metric::find_index_before_or_on(TimePoint t, uint64_t left, uint64_t right)
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
        return find_index_before_or_on(t, pivot, right);
    }
    else
    {
        return find_index_before_or_on(t, left, pivot);
    }
}

uint64_t Metric::find_index_on_or_after(TimePoint t, uint64_t left, uint64_t right)
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
        return find_index_on_or_after(t, pivot, right);
    }
    else
    {
        return find_index_on_or_after(t, left, pivot);
    }
}

TimePoint Metric::get_raw_ts(uint64_t index)
{
    return file_raw().read(index).time;
}

uint64_t Metric::size()
{
    return file_raw().size();
}

uint64_t Metric::size(Duration interval)
{
    return file_hta(interval).size();
}

std::pair<uint64_t, uint64_t> Metric::find_index(TimePoint begin, TimePoint end,
                                                 IntervalScope scope)
{
    const auto sz = size();
    if (sz == 0)
    {
        return { 0, 0 };
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
        index_begin = find_index_on_or_after(begin, 0, sz);
        break;
    case Scope::open:
        index_begin = find_index_before_or_on(begin, 0, sz);
        if (index_begin < sz)
        {
            index_begin++;
        }
        break;
    case Scope::extended:
        index_begin = find_index_before_or_on(begin, 0, sz);
        break;
    case Scope::infinity:
        index_begin = 0;
        break;
    }

    switch (scope.end)
    {
    case Scope::closed:
        index_end = find_index_before_or_on(end, 0, sz);
        break;
    case Scope::open:
        index_end = find_index_on_or_after(end, 0, sz);
        if (index_end > 0)
        {
            index_end--;
        }
        break;
    case Scope::extended:
        index_end = find_index_on_or_after(end, 0, sz);
        break;
    case Scope::infinity:
        index_end = sz - 1;
        break;
    }
    // Move index_end to *after* the last included element... if not already outside.
    if (index_end < sz)
    {
        index_end++;
    }
    return { index_begin, index_end };
}

std::vector<TimeValue> Metric::get(TimePoint begin, TimePoint end, IntervalScope scope)
{
    auto[index_begin, index_end] = find_index(begin, end, scope);
    if (index_begin == index_end)
    {
        return {};
    }
    std::vector<TimeValue> result(index_end - index_begin);
    file_raw().read(result, index_begin);
    return result;
}

size_t Metric::count(TimePoint begin, TimePoint end, IntervalScope scope)
{
    auto[index_begin, index_end] = find_index(begin, end, scope);
    return index_end - index_begin;
}

TimePoint Metric::epoch()
{
    // TODO cache!
    return file_raw().read(0).time;
}

TimePoint Metric::epoch(Duration interval)
{
    return interval_begin(epoch(), interval);
}

std::vector<TimeAggregate> Metric::get(TimePoint begin, TimePoint end, Duration interval,
                                       IntervalScope scope)
{
    auto sz = size();
    if (sz == 0)
    {
        return {};
    }

    auto& file = file_hta(interval);

    assert(end >= begin || scope.begin == Scope::infinity || scope.end == Scope::infinity);

    auto epoch_ = epoch(interval);
    auto offset_begin = begin - epoch_;
    auto offset_end = end - epoch_;

    int64_t index_begin;
    int64_t index_end; // this point is **included** in the result!
    switch (scope.begin)
    {
    case Scope::closed:
        index_begin = (offset_begin - Duration(1)) / interval + 1;
        break;
    case Scope::open:
        index_begin = offset_begin / interval + 1;
        break;
    case Scope::extended:
        index_begin = offset_begin / interval;
        break;
    case Scope::infinity:
        index_begin = 0;
        break;
    }

    switch (scope.end)
    {
    case Scope::closed:
        index_end = offset_end / interval;
        break;
    case Scope::open:
        index_end = (offset_end - Duration(1)) / interval;
        break;
    case Scope::extended:
        index_end = (offset_end - Duration(1)) / interval + 1;
        break;
    case Scope::infinity:
        index_end = sz - 1;
        break;
    }

    index_begin = std::max<int64_t>(index_begin, 0);
    index_end = std::min<int64_t>(index_end, sz);

    if (index_begin >= sz || index_end < 0)
    {
        return {};
    }

    size_t count = index_end - index_begin + 1;

    std::vector<TimeAggregate> result(count);
    file.read(result, index_begin);

#ifndef NDEBUG
    // Check consistency of times
    TimePoint t = epoch_ + index_begin * interval;
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
    return file_raw().read(size() - 1);
}

TimeAggregate Metric::last(Duration interval)
{
    return file_hta(interval).read(size(interval) - 1);
}

std::pair<TimePoint, TimePoint> Metric::range()
{
    TimeValue first = file_raw().read(0);
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

Metric::HtaFile& Metric::file_hta(Duration interval)
{
    auto it = files_hta_.find(interval);
    if (it == files_hta_.end())
    {
        bool added;
        std::tie(it, added) = files_hta_.try_emplace(interval, FileOpenTag::ReadWrite(),
                                                     path_hta(interval), Header(interval, meta()));
        assert(added);
    }
    return it->second;
}
}
