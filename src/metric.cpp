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
#include "storage/metric.hpp"
#include "level.hpp"

#include <hta/exception.hpp>
#include <hta/meta.hpp>
#include <hta/metric.hpp>
#include <hta/ostream.hpp>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include <cassert>

namespace hta
{
/*
 * common methods
 */

Metric::Metric(std::unique_ptr<storage::Metric> storage_metric)
: storage_metric_(std::move(storage_metric)), interval_min_(storage_metric_->meta().interval_min),
  interval_max_(storage_metric_->meta().interval_max),
  interval_factor_(storage_metric_->meta().interval_factor),
  previous_time_(storage_metric_->range().second)
{
    if (interval_min_ > interval_max_)
    {
        throw_exception("interval_min > interval_max");
    }
}

Metric::Metric(hta::Metric&&) = default;
Metric& Metric::operator=(hta::Metric&&) = default;
Metric::~Metric() = default;

const Meta Metric::meta() const
{
    return storage_metric_->meta();
}

void Metric::check_read() const
{
    switch (storage_metric_->mode())
    {
    case Mode::read:
    case Mode::read_write:
        return;
    case Mode::write:
        throw Exception("attempting to read from metric opened for write only.");
    }
}

void Metric::check_write() const
{
    switch (storage_metric_->mode())
    {
    case Mode::write:
    case Mode::read_write:
        return;
    case Mode::read:
        throw Exception("attempting to write to metric opened for read only.");
    }
}

/*
 * read methods
 */

std::vector<Row> Metric::retrieve_raw_row(TimePoint begin, TimePoint end, IntervalScope scope)
{
    auto result_tv = storage_metric_->get(begin, end, scope);
    std::vector<Row> result;
    result.reserve(result_tv.size());
    for (auto tv : result_tv)
    {
        result.emplace_back(Duration(0), tv.time, Aggregate(tv.value));
    }
    return result;
}

std::vector<TimeValue> Metric::retrieve(TimePoint begin, TimePoint end, IntervalScope scope)
{
    check_read();
    if (begin > end && scope.begin != Scope::infinity && scope.end != Scope::infinity)
    {
        throw_exception("invalid request: begin timestamp ", begin, " larger than end timestamp ",
                        end);
    }
    return storage_metric_->get(begin, end, scope);
}

Aggregate Metric::aggregate(hta::TimePoint begin, hta::TimePoint end)
{
    check_read();
    if (begin >= end)
    {
        throw_exception("invalid request: begin timestamp ", begin, " larger than end timestamp ",
                        end);
    }
    // We must cap the end or we use intervals that are not yet completely written
    end = std::min(end, range().second);

    auto interval = interval_min_;

    auto next_begin = interval_end(begin - Duration(1), interval);
    auto next_end = interval_begin(end, interval);

    Aggregate a;

    if (next_begin >= next_end)
    {
        // No need to go to any intervals or do any splits, just one raw chunk
        auto raw =
            storage_metric_->get(begin, next_begin, IntervalScope{ Scope::open, Scope::extended });

        auto previous_time = begin;
        for (auto tv : raw)
        {
            assert(previous_time <= tv.time);
            if (tv.time > end)
            {
                tv.time = end;
            }
            a += Aggregate(tv.value, tv.time - previous_time);
            previous_time = tv.time;
        }
        return a;
    }
    assert(begin <= next_begin);
    if (begin < next_begin)
    {
        // Add left raw side
        auto left_raw =
            storage_metric_->get(begin, next_begin, IntervalScope{ Scope::open, Scope::extended });

        auto previous_time = begin;
        for (auto tv : left_raw)
        {
            assert(previous_time <= tv.time);
            if (tv.time > next_begin)
            {
                // We add this for the integral but the point isn't actually in
                auto partial_duration = next_begin - previous_time;
                Aggregate partial_interval{
                    tv.value, tv.value, 0, 0, tv.value * partial_duration.count(), partial_duration
                };
                a += partial_interval;
            }
            else
            {
                a += Aggregate(tv.value, tv.time - previous_time);
            }
            previous_time = tv.time;
        }
    }
    assert(next_end <= end);
    if (next_end < end)
    {
        // Add right raw side
        auto right_raw =
            storage_metric_->get(next_end, end, IntervalScope{ Scope::open, Scope::extended });
        auto previous_time = next_end;
        for (auto tv : right_raw)
        {
            if (tv.time > end)
            {
                // We add this for the integral but the point isn't actually in
                auto partial_duration = end - previous_time;
                Aggregate partial_interval{
                    tv.value, tv.value, 0, 0, tv.value * partial_duration.count(), partial_duration
                };
                a += partial_interval;
            }
            else
            {
                a += Aggregate(tv.value, tv.time - previous_time);
            }
        }
    }

    while (true)
    {
        auto next_interval = interval * interval_factor_;
        next_begin = interval_end(begin - Duration(1), next_interval);
        next_end = interval_begin(end, next_interval);

        if (next_interval > interval_max_ ||
            interval_end(begin, next_interval) >= interval_begin(end, next_interval))
        {
            // Use contiguous block and end
            auto rows = storage_metric_->get(begin, interval_begin(end, interval), interval,
                                             IntervalScope{ Scope::closed, Scope::open });
            for (const auto& ta : rows)
            {
                a += ta.aggregate;
            }
            break;
        }

        // add left aggregates
        auto rows_left = storage_metric_->get(begin, next_begin, interval,
                                              IntervalScope{ Scope::closed, Scope::open });
        for (const auto& ta : rows_left)
        {
            a += ta.aggregate;
        }

        // add right aggregates
        auto rows_right = storage_metric_->get(next_end, interval_begin(end, interval), interval,
                                               IntervalScope{ Scope::closed, Scope::open });
        for (const auto& ta : rows_right)
        {
            a += ta.aggregate;
        }

        interval = next_interval;
    }
    return a;
}

size_t Metric::count(TimePoint begin, TimePoint end, IntervalScope scope)
{
    check_read();
    return storage_metric_->count(begin, end, scope);
}

size_t Metric::count()
{
    check_read();
    return storage_metric_->size();
}

std::vector<Row> Metric::retrieve(TimePoint begin, TimePoint end, uint64_t min_samples,
                                  IntervalScope scope)
{
    check_read();
    if (begin > end)
    {
        throw_exception("invalid request: begin timestamp ", begin, " larger than end timestamp ",
                        end);
    }
    auto duration = end - begin;
    auto interval_max_length = duration / min_samples;
    return retrieve(begin, end, interval_max_length, scope);
}

std::vector<Row> Metric::retrieve(TimePoint begin, TimePoint end, Duration interval_upper_limit,
                                  IntervalScope scope)
{
    check_read();
    if (begin > end && scope.begin != Scope::infinity && scope.end != Scope::infinity)
    {
        throw_exception("invalid request: begin timestamp ", begin, " larger than end timestamp ",
                        end);
    }
    if (interval_upper_limit < interval_min_)
    {
        return retrieve_raw_row(begin, end, scope);
    }
    auto interval = interval_min_;
    interval_upper_limit = std::min(interval_upper_limit, interval_max_);
    while (interval * interval_factor_ <= interval_upper_limit)
    {
        interval = interval * interval_factor_;
    }
    do
    {
        auto result = storage_metric_->get(begin, end, interval, scope);
        if (!result.empty())
        {
            std::vector<Row> rows;
            rows.reserve(result.size());
            std::transform(result.begin(), result.end(), std::back_inserter(rows),
                           [interval](TimeAggregate ta) -> Row {
                               return { interval, ta };
                           });
            return rows;
        }
        // If the requested level has no data yet, we must ask the lower levels
        interval = interval / interval_factor_;
    } while (interval >= interval_min_);
    // No data at all
    return {};
}

std::pair<TimePoint, TimePoint> Metric::range()
{
    check_read();
    return storage_metric_->range();
}

/*
 * write methods
 */

Level Metric::restore_level(Duration interval)
{
    Level level;
    if (storage_metric_->size() == 0)
    {
        // No need to restore anything.
        return level;
    }

    // How much did we already store here?
    TimePoint time_closed;
    Scope scope_begin = Scope::infinity;
    if (storage_metric_->size(interval))
    {
        time_closed = storage_metric_->last(interval).time + interval;
        scope_begin = Scope ::closed;
        level.time_current = time_closed;
    }

    if (interval == interval_min_)
    {
        auto data =
            storage_metric_->get(time_closed, {}, IntervalScope{ scope_begin, Scope::infinity });
        if (!data.empty())
        {
            if (!level.time_current)
            {
                level.time_current = data[0].time;
            }
            for (auto& tv : data)
            {
                level.advance(tv);
            }
        }
    }
    else
    {
        auto smaller_interval = interval / interval_factor_;
        auto data = storage_metric_->get(time_closed, {}, smaller_interval,
                                         IntervalScope{ scope_begin, Scope::infinity });
        for (auto& ta : data)
        {
            level.advance({ ta.time + smaller_interval, ta.aggregate });
        }
    }
    return level;
}

Level& Metric::get_level(Duration interval)
{
    auto it = levels_.find(interval);
    if (it == levels_.end())
    {
        bool added;
        std::tie(it, added) = levels_.try_emplace(interval, restore_level(interval));
    }
    return it->second;
}

void Metric::insert(TimeValue tv)
{
    check_write();
    // Evaluate if these two consistency checks hurt performance
    // Must not have an "invalid" time value... who knows what would happen...
    if (!tv.time)
    {
        throw std::out_of_range("cannot insert invalid (0) hta::TimePoint");
    }
    if (tv.time <= previous_time_)
    {
        throw std::out_of_range("trying to add non-monotonic timestamp; previous " +
                                std::to_string(previous_time_.time_since_epoch().count()) +
                                " new " + std::to_string(tv.time.time_since_epoch().count()));
    }
    previous_time_ = tv.time;

    const auto interval = interval_min_;
    auto& level = get_level(interval);
    // We must do this after get_level, otherwise it confuses the level restore
    storage_metric_->insert(tv);

    if (!level.time_current)
    {
        // level.time_current = interval_begin(tv.time, interval);
        level.time_current = tv.time;
    }
    auto level_time_end = interval_end(level.time_current, interval);
    while (tv.time >= level_time_end)
    {
        // the point doesn't belong in the current interval,
        // but some of it's integral may..
        auto partial_duration = level_time_end - level.time_current;
        assert(partial_duration <= interval);
        // TODO move this into a clever method of Aggregate itself
        Aggregate partial_interval{
            tv.value, tv.value, 0, 0, tv.value * partial_duration.count(), partial_duration
        };
        level.advance({ level_time_end, partial_interval });

        auto level_time_begin = level_time_end - interval;

        // Inform higher levels of this new closed level
        insert({ interval, level_time_begin, level.aggregate });

        // reset the interval at lowest level
        level = Level(level_time_end);
        level_time_end = interval_end(level.time_current, interval);
    }
    level.advance(tv);
}

void Metric::insert(Row row)
{
    const auto interval = row.interval * interval_factor_;
    if (interval > interval_max_)
    {
        // write this but don't do anything else!
        storage_metric_->insert(row);
        return;
    }
    auto& level = get_level(interval);
    // We must do this after get_level, otherwise it confuses the level restore
    storage_metric_->insert(row);
    if (!level.time_current)
    {
        level.time_current = row.time;
    }
    else if (level.time_current != row.time)
    {
        throw_exception("inconsistent level time for interval ", interval,
                        " time_current: ", level.time_current, " row.time: ", row.time);
    }
    auto level_time_end = interval_end(level.time_current, interval);
    if (row.end_time() >= level_time_end)
    {
        // the new row from below completes the interval at current level
        // Small intervals should never cross through multiple larger ones
        assert(row.end_time() == level_time_end);
        // Close the interval and make a new one
        level.advance({ level_time_end, row.aggregate });

        auto level_time_begin = level_time_end - interval;

        // Inform higher levels of this new closed level
        insert({ interval, level_time_begin, level.aggregate });

        // reset the interval at current level
        level = Level(level_time_end);
        return;
    }

    level.advance({ row.end_time(), row.aggregate });
}

void Metric::flush()
{
    check_write();
    storage_metric_->flush();
}
} // namespace hta
