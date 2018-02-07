#pragma once

#include <types.hpp>

#include <vector>

namespace hta::storage
{
class Metric
{
public:
    virtual void insert(TimeValue tv) = 0;
    virtual void insert(Row row) = 0;

    virtual void truncate() = 0;
    virtual void truncate(Duration interval) = 0;

    virtual void flush() = 0;

    virtual std::vector<TimeValue> get(TimePoint t0, TimePoint t1,
                                       IntervalScope scope = IntervalScope::CLOSED_EXTENDED) = 0;
    virtual std::vector<Row> get(TimePoint t0, TimePoint t1, Duration interval,
                                 IntervalScope scope = IntervalScope::CLOSED_EXTENDED) = 0;

    virtual std::pair<TimePoint, TimePoint> range() = 0;

    virtual ~Metric(){};
};
}
