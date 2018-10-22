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

#include <hta/metric.hpp>
#include <hta/types.hpp>

#include <algorithm>
#include <utility>
#include <vector>

#include <cassert>

namespace hta
{
ReadMetric::ReadMetric()
{
    // just to be sure that we are initialized properly
    assert(storage_metric_);
    previous_time_ = storage_metric_->range().second;
}

ReadMetric::ReadMetric(std::unique_ptr<storage::Metric> storage_metric)
: BaseMetric(std::move(storage_metric))
{
    previous_time_ = storage_metric_->range().second;
}

std::vector<Row> ReadMetric::retrieve_raw_row(TimePoint begin, TimePoint end, IntervalScope scope)
{
    auto result_tv = storage_metric_->get(begin, end, scope);
    std::vector<Row> result;
    result.reserve(result_tv.size());
    for (auto tv : result_tv)
    {
        result.push_back({ Duration(0), tv.time, Aggregate(tv.value) });
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

size_t ReadMetric::count()
{
    return storage_metric_->size();
}

std::vector<Row> ReadMetric::retrieve(TimePoint begin, TimePoint end, uint64_t min_samples,
                                      IntervalScope scope)
{
    assert(begin <= end);
    auto duration = end - begin;
    auto interval_max_length = duration / min_samples;
    return retrieve(begin, end, interval_max_length, scope);
}

std::vector<Row> ReadMetric::retrieve(TimePoint begin, TimePoint end, Duration interval_upper_limit,
                                      IntervalScope scope)
{
    if (interval_upper_limit < interval_min_)
    {
        return retrieve_raw_row(begin, end, scope);
    }
    auto interval = interval_min_;
    while (interval * interval_factor_ <= interval_upper_limit)
    {
        interval = interval * interval_factor_;
    }
    do
    {
        auto result = storage_metric_->get(begin, end, interval, scope);
        if (result.size() > 0)
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

std::pair<TimePoint, TimePoint> ReadMetric::range()
{
    return storage_metric_->range();
}
} // namespace hta
