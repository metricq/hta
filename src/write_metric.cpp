#include "level.hpp"

#include "storage/metric.hpp"

#include <hta/metric.hpp>
#include <hta/types.hpp>

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
    // Must not have an "invalid" time value... who knows what would happen...
    assert(tv.time);

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
    auto& level = get_level(interval);
    // We must do this after get_level, otherwise it confuses the level restore
    storage_metric_->insert(row);
    if (!level.time_current)
    {
        level.time_current = row.end_time();
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
}
