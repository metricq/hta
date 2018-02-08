#include "storage/metric.hpp"

#include <metric.hpp>

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

void Metric::insert(TimeValue tv)
{
    // Must not have an "invalid" time value... who knows what would happen...
    assert(tv.time);
    storage_metric_->insert(tv);

    const auto interval = interval_min_;
    auto& level = levels_[interval];
    if (!level.time_current)
    {
        level.time_current = interval_begin(tv.time, interval);
    }
    auto level_time_end = interval_end(level.time_current, interval);
    if (tv.time >= level_time_end)
    {
        // the point doesn't belong in the current interval,
        // but some of it's integral may..
        auto partial_duration = level_time_end - level.time_current;
        assert(partial_duration <= interval);
        // TODO move this into a clever method of Aggregate itself
        Aggregate partial_interval{
            tv.value, tv.value, 0, 0, tv.value * partial_duration.count(), partial_duration
        };
        level.advance({ tv.time, partial_interval });

        auto level_time_begin = level_time_end - interval;

        // Inform higher levels of this new closed level
        insert({ interval, level_time_begin, level.aggregate });

        // reset the interval at lowest level
        level = Level(level_time_end);

        // Try to add it again
        // TODO make this tail recursive to avoid stack overflow
        insert(tv);
        return;
    }
    level.advance(tv);
}

void Metric::insert(Row row)
{
    storage_metric_->insert(row);

    const auto interval = row.interval;
    auto& level = levels_[row.interval];
    if (!level.time_current)
    {
        level.time_current = interval_begin(row.time, interval);
    }
    auto level_time_end = interval_end(level.time_current, row.interval);
    if (row.time >= level_time_end)
    {
        // the new row from below completes the interval at current level
        // Small intervals should never cross through multiple larger ones
        assert(row.time == level_time_end);
        // Close the interval and make a new one
        level.advance({ level_time_end, row.aggregate });

        auto level_time_begin = level_time_end - interval;

        // Inform higher levels of this new closed level
        insert({ interval, level_time_begin, level.aggregate });

        // reset the interval at current level
        level = Level(level_time_end);
        // recurse to gracefully handle current_ts being multiple intervals behind
        // TODO avoid stackoverflow...
        insert(row);
        return;
    }

    level.advance({ row.time, row.aggregate });
}

std::vector<TimeAggregate> Metric::retrieve_raw(TimePoint begin, TimePoint end, IntervalScope scope)
{
    std::vector<TimeAggregate> result;
    auto result_tv = storage_metric_->get(begin, end, scope);
    result.reserve(result_tv.size());
    for (auto tv : result_tv)
    {
        result.push_back({tv.time, Aggregate(tv.value)});
    }
    return result;
}

std::vector<TimeAggregate> Metric::retrieve(TimePoint begin, TimePoint end, uint64_t min_samples,
                                            IntervalScope scope)
{
    assert(begin <= end);
    auto duration = end - begin;
    auto interval_max_length = duration / min_samples;
    if (interval_max_length < interval_min_)
    {
        return retrieve_raw(begin, end, scope);
    }
    auto interval = interval_min_;
    while (interval * interval_factor_ <= interval_max_length)
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
}
