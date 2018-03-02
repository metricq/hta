#include "storage/metric.hpp"

#include <hta/metric.hpp>
#include <hta/types.hpp>

#include <utility>
#include <vector>

#include <cassert>

namespace hta
{
ReadMetric::ReadMetric()
{
    // just to be sure that we are initialized properly
    assert(storage_metric_);
}

ReadMetric::ReadMetric(std::unique_ptr<storage::Metric> storage_metric)
: BaseMetric(std::move(storage_metric))
{
}

std::vector<TimeAggregate> ReadMetric::retrieve_raw_time_aggregate(TimePoint begin, TimePoint end,
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

std::vector<TimeValue> ReadMetric::retrieve(TimePoint begin, TimePoint end, IntervalScope scope)
{
    return storage_metric_->get(begin, end, scope);
}

size_t ReadMetric::count(TimePoint begin, TimePoint end, IntervalScope scope)
{
    return storage_metric_->count(begin, end, scope);
}

std::vector<TimeAggregate> ReadMetric::retrieve(TimePoint begin, TimePoint end,
                                                uint64_t min_samples, IntervalScope scope)
{
    assert(begin <= end);
    auto duration = end - begin;
    auto interval_max_length = duration / min_samples;
    return retrieve(begin, end, interval_max_length, scope);
}

std::vector<TimeAggregate> ReadMetric::retrieve(TimePoint begin, TimePoint end,
                                                Duration interval_max, IntervalScope scope)
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

std::pair<TimePoint, TimePoint> ReadMetric::range()
{
    return storage_metric_->range();
}
}
