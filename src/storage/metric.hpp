#pragma once

#include <types.hpp>

#include <cstdint>
#include <vector>

namespace hta::storage
{
class Metric
{
public:
    virtual void insert(TimeValue tv) = 0;
    virtual void insert(Row row) = 0;

    virtual void flush() = 0;

    virtual std::vector<TimeValue> get(TimePoint t0, TimePoint t1, IntervalScope scope) = 0;
    virtual std::vector<TimeAggregate> get(TimePoint t0, TimePoint t1, Duration interval,
                                           IntervalScope scope) = 0;

    virtual TimeValue last() = 0;
    virtual TimeAggregate last(Duration interval) = 0;

    virtual std::size_t size() = 0;
    virtual std::size_t size(Duration interval) = 0;

    virtual std::pair<TimePoint, TimePoint> range() = 0;

    virtual ~Metric(){};
};
}
