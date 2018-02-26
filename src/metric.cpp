#include "storage/metric.hpp"

#include <hta/metric.hpp>

#include <cassert>

namespace hta
{
class Level
{
public:
    Level() = default;
    Level(TimePoint time) : time_current(time)
    {
    }

    void advance(TimeValue tv)
    {
        assert(tv.time >= time_current);
        auto duration = tv.time - time_current;
        aggregate += { tv.value, duration };
        time_current = tv.time;
    }

    void advance(TimeAggregate ta)
    {
        assert(ta.time >= time_current);
        aggregate += ta.aggregate;
        time_current = ta.time;
    }

    // The lowest level uses this for integrals
    // For higher levels, this is only used for consistency checks and refers to the end
    // of the most recent consumed row
    TimePoint time_current;
    Aggregate aggregate;
};

Metric::Metric(std::unique_ptr<storage::Metric> storage_metric)
: storage_metric_(std::move(storage_metric))
{
}

Metric::~Metric()
{
}

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

void Metric::insert(Row row)
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

std::vector<TimeAggregate> Metric::retrieve_raw_time_aggregate(TimePoint begin, TimePoint end,
                                                               IntervalScope scope)
{
    auto result_tv = storage_metric_->get(begin, end, scope);
    std::vector<TimeAggregate> result;
    result.reserve(result_tv.size());
    for (auto tv : result_tv)
    {
        result.push_back({ tv.time, Aggregate(tv.value) });
    }
    return result;
}

std::vector<TimeValue> Metric::retrieve(TimePoint begin, TimePoint end, IntervalScope scope)
{
    return storage_metric_->get(begin, end, scope);
}

std::vector<TimeAggregate> Metric::retrieve(TimePoint begin, TimePoint end, uint64_t min_samples,
                                            IntervalScope scope)
{
    assert(begin <= end);
    auto duration = end - begin;
    auto interval_max_length = duration / min_samples;
    return retrieve(begin, end, interval_max_length, scope);
}

std::vector<TimeAggregate> Metric::retrieve(TimePoint begin, TimePoint end, Duration interval_max,
                                            IntervalScope scope)
{
    if (interval_max < interval_min_)
    {
        return retrieve_raw_time_aggregate(begin, end, scope);
    }
    auto interval = interval_min_;
    while (interval * interval_factor_ <= interval_max)
    {
        interval *= interval_factor_;
    }
    do
    {
        auto result = storage_metric_->get(begin, end, interval, scope);
        if (result.size() > 0)
        {
            return result;
        }
        // If the requested level has no data yet, we must ask the lower levels
        interval /= interval_factor_;
    } while (interval < interval_min_);
    // No data at all
    return {};
}

std::pair<TimePoint, TimePoint> Metric::range()
{
    return storage_metric_->range();
}
}
