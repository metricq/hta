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
#include "metric.hpp"

#include <hta/filesystem.hpp>
#include <hta/types.hpp>

#include <algorithm>

namespace hta::storage::file
{
Meta Metric::meta() const
{
    return Meta{
        Duration(file_raw_.header().interval_min),
        Duration(file_raw_.header().interval_max),
        file_raw_.header().interval_factor,
    };
}

Mode Metric::mode() const
{
    return open_mode_;
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

int64_t Metric::find_index_before_or_on_binary(TimePoint t, int64_t left, int64_t right, int64_t sz)
{
    // left < right means right != 0
    // assumes monotonicity ts(left) < ts(right)
    // invariant ts(left) <= t < ts(right)
    // right can be size() // out of range, but the pivot can never be
    assert(left < right);
    // TODO expensive, use extra debug macro
    assert(get_raw_ts(left) <= t);
    assert(right == sz || t < get_raw_ts(right));
    if (right - left == 1)
    {
        return left;
    }
    auto pivot = (left + right) / 2;
    assert(pivot < sz);
    TimePoint t_pivot = get_raw_ts(pivot);
    if (t >= t_pivot)
    {
        return find_index_before_or_on_binary(t, pivot, right, sz);
    }
    else
    {
        return find_index_before_or_on_binary(t, left, pivot, sz);
    }
}

int64_t Metric::find_index_before_or_on(TimePoint t, int64_t sz)
{
    auto [begin, end] = range();
    if (begin > t)
    {
        return -1;
    }
    if (end <= t)
    {
        return sz - 1;
    }

    int64_t index = 0;
    auto m = meta();
    Duration interval = m.interval_min;

    while (true)
    {
        auto interval_next = interval * m.interval_factor;
        auto interval_begin_current = interval_begin(t, interval);
        auto interval_begin_next = interval_begin(t, interval_next);

        if (interval_next > m.interval_max)
        {
            interval_begin_next = epoch(interval);
        }

        if (interval_begin_current != interval_begin_next)
        {
            auto level = this->get(interval_begin_next, interval_begin_current, interval,
                                   { Scope::closed, Scope::open });
            for (const auto& chunk : level)
            {
                index += chunk.aggregate.count;
            }
        }

        if (interval_begin_next <= epoch(interval))
        {
            break;
        }
        interval = interval_next;
    }

    // >= index points are now *before* t
    for (; index + 1 < sz && get_raw_ts(index + 1) <= t; index++)
        ;

    assert(get_raw_ts(index) <= t);
    assert(index == int64_t(sz - 1) || get_raw_ts(index + 1) > t);
    return index;
}

int64_t Metric::find_index_on_or_after_binary(TimePoint t, int64_t left, int64_t right, int64_t sz)
{
    // left < right means right != 0
    // assumes monotonicity ts(left) < ts(right)
    // invariant ts(left) < t <= ts(right)
    // right can be size() // out of range, but the pivot can never be
    assert(left < right);
    // TODO expensive, use extra debug macro
    assert(get_raw_ts(left) < t);
    assert(right == sz || t <= get_raw_ts(right));

    if (right - left == 1)
    {
        return right;
    }
    auto pivot = (left + right) / 2;
    assert(pivot < sz);

    TimePoint t_pivot = get_raw_ts(pivot);
    if (t > t_pivot)
    {
        return find_index_on_or_after_binary(t, pivot, right, sz);
    }
    else
    {
        return find_index_on_or_after_binary(t, left, pivot, sz);
    }
}

int64_t Metric::find_index_on_or_after(TimePoint t, int64_t sz)
{
    auto [begin, end] = range();
    if (begin >= t)
    {
        return 0;
    }
    if (end < t)
    {
        return sz;
    }

    int64_t index = 0;
    auto m = meta();
    Duration interval = m.interval_min;

    while (true)
    {
        auto interval_next = interval * m.interval_factor;
        auto interval_begin_current = interval_begin(t, interval);
        auto interval_begin_next = interval_begin(t, interval_next);

        if (interval_next > m.interval_max)
        {
            interval_begin_next = epoch(interval);
        }

        if (interval_begin_current != interval_begin_next)
        {
            auto level = this->get(interval_begin_next, interval_begin_current, interval,
                                   { Scope::closed, Scope::open });
            for (const auto& chunk : level)
            {
                index += chunk.aggregate.count;
            }
        }

        if (interval_begin_next <= epoch(interval))
        {
            break;
        }
        interval = interval_next;
    }

    // >= index points are now *before* t
    for (; index < sz && get_raw_ts(index) < t; index++)
        ;

    assert(index == 0 || get_raw_ts(index - 1) < t);
    assert(get_raw_ts(index) >= t);
    return index;
}

TimePoint Metric::get_raw_ts(uint64_t index)
{
    return file_raw().read(index).time;
}

std::size_t Metric::size()
{
    return file_raw().size();
}

std::size_t Metric::size(Duration interval)
{
    return file_hta(interval).size();
}

std::pair<uint64_t, uint64_t> Metric::find_index(TimePoint begin, TimePoint end,
                                                 IntervalScope scope)
{
    // Must be signed for comparision
    const auto sz = static_cast<int64_t>(size());
    if (sz == 0)
    {
        return { 0, 0 };
    }

    // We initialize so we don't get uninitialized
    int64_t index_begin = -1;
    /* this is the index of the end _element_ (not after!) according to scope.end
     * It may be out of scope (==size()) in some cases */
    int64_t index_end = -1;
    // This is quite complex and possibly bad for performance
    // TODO optimize // simplify
    switch (scope.begin)
    {
    case Scope::closed:
        index_begin = find_index_on_or_after(begin, sz);
        break;
    case Scope::open:
        index_begin = find_index_before_or_on(begin, sz);
        if (index_begin < sz)
        {
            index_begin++;
        }
        break;
    case Scope::extended:
        index_begin = find_index_before_or_on(begin, sz);
        index_begin = std::max<decltype(index_begin)>(index_begin, 0);
        break;
    case Scope::infinity:
        index_begin = 0;
        break;
    }

    switch (scope.end)
    {
    case Scope::closed:
        index_end = find_index_before_or_on(end, sz);
        break;
    case Scope::open:
        index_end = find_index_on_or_after(end, sz);
        if (index_end > 0)
        {
            index_end--;
        }
        break;
    case Scope::extended:
        index_end = find_index_on_or_after(end, sz);
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

    assert(index_begin >= 0);
    assert(index_begin < sz);
    assert(index_end >= 0);
    assert(index_end <= sz);
    assert(index_begin <= index_end);

    return { index_begin, index_end };
}

std::vector<TimeValue> Metric::get(TimePoint begin, TimePoint end, IntervalScope scope)
{
    auto [index_begin, index_end] = find_index(begin, end, scope);
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
    auto [index_begin, index_end] = find_index(begin, end, scope);
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
    auto sz = static_cast<int64_t>(size(interval));
    if (sz == 0)
    {
        return {};
    }

    HtaFile* file;
    try
    {
        file = &file_hta(interval);
    }
    catch (Exception& e)
    {
        if (e.error_number() == ENOENT)
        {
            // File does not exist... no data in this level
            // TODO handle this somehow better with meta or so
            return {};
        }
        throw;
    }

    assert(end >= begin || scope.begin == Scope::infinity || scope.end == Scope::infinity);

    auto epoch_ = epoch(interval);
    auto offset_begin = begin - epoch_;
    auto offset_end = end - epoch_;

    // We initialize so we don't get uninitialized warnings
    int64_t index_begin = -1;
    int64_t index_end = -1; // this point is **included** in the result!
    switch (scope.begin)
    {
    case Scope::closed:
        // Unfortunately division with negative numbers does *not* round down
        // so we have to make extra cases for pre-epoch
        if (offset_begin.count() <= 0)
        {
            index_begin = 0;
        }
        else
        {
            index_begin = (offset_begin - Duration(1)) / interval + 1;
        }
        break;
    case Scope::open:
        if (offset_begin.count() < 0)
        {
            index_begin = 0;
        }
        else
        {
            index_begin = offset_begin / interval + 1;
        }
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
        if (offset_end.count() <= 0)
        {
            index_end = 0;
        }
        else
        {
            index_end = (offset_end - Duration(1)) / interval + 1;
        }
        break;
    case Scope::infinity:
        index_end = sz - 1;
        break;
    }

    index_begin = std::max<int64_t>(index_begin, 0);
    index_end = std::min<int64_t>(index_end, sz);

    if (index_begin >= static_cast<int64_t>(sz) || index_end < 0)
    {
        return {};
    }

    assert(index_begin >= 0);
    assert(index_begin < sz);
    assert(index_end >= 0);
    assert(index_end <= sz);
    assert(index_begin <= index_end);

    size_t count = index_end - index_begin + 1;

    std::vector<TimeAggregate> result(count);
    file->read(result, index_begin);

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
    if (file_raw().size() == 0)
    {
        return { TimePoint(), TimePoint() };
    }
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
        switch (open_mode_)
        {
        case Mode::read:
            std::tie(it, added) =
                files_hta_.try_emplace(interval, FileOpenTag::Read(), path_hta(interval));
            break;
        case Mode::write:
            std::tie(it, added) = files_hta_.try_emplace(
                interval, FileOpenTag::Write(), path_hta(interval), Header(interval, meta()));
            break;
        case Mode::read_write:
            std::tie(it, added) = files_hta_.try_emplace(
                interval, FileOpenTag::ReadWrite(), path_hta(interval), Header(interval, meta()));
            break;
        }
        assert(added);
    }
    return it->second;
}
} // namespace hta::storage::file
