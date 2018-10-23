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
#include "level.hpp"

#include "storage/metric.hpp"

#include <hta/metric.hpp>
#include <hta/types.hpp>

#include <stdexcept>
#include <tuple>

#include <cassert>

namespace hta
{
WriteMetric::WriteMetric()
{
    // just to be sure that we are initialized properly
    assert(storage_metric_);
}

WriteMetric::~WriteMetric()
{
}

WriteMetric::WriteMetric(std::unique_ptr<storage::Metric> storage_metric)
: BaseMetric(std::move(storage_metric))
{
}

Level WriteMetric::restore_level(Duration interval)
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
        if (data.size() > 0)
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

Level& WriteMetric::get_level(Duration interval)
{
    auto it = levels_.find(interval);
    if (it == levels_.end())
    {
        bool added;
        std::tie(it, added) = levels_.try_emplace(interval, restore_level(interval));
    }
    return it->second;
}

void WriteMetric::insert(TimeValue tv)
{
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

void WriteMetric::insert(Row row)
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
    else
    {
        assert(level.time_current == row.time);
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

void WriteMetric::flush()
{
    storage_metric_->flush();
}
} // namespace hta
