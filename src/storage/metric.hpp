#pragma once

#include <hta/types.hpp>
#include <hta/meta.hpp>

#include <cstdint>
#include <vector>

namespace hta::storage
{
class json;

class Metric
{
public:
    virtual Meta meta() const = 0;

    virtual void insert(TimeValue tv) = 0;
    virtual void insert(Row row) = 0;

    virtual void flush() = 0;

    virtual std::vector<TimeValue> get(TimePoint begin, TimePoint end, IntervalScope scope) = 0;
    virtual std::vector<TimeAggregate> get(TimePoint begin, TimePoint end, Duration interval,
                                           IntervalScope scope) = 0;

    virtual size_t count(TimePoint begin, TimePoint end, IntervalScope scope) = 0;

    virtual TimeValue last() = 0;
    virtual TimeAggregate last(Duration interval) = 0;

    virtual std::size_t size() = 0;
    virtual std::size_t size(Duration interval) = 0;

    virtual std::pair<TimePoint, TimePoint> range() = 0;

    virtual ~Metric(){};
};
}
