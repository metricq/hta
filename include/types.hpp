#pragma once

#include "chrono.hpp"

namespace hta
{

using Value = double;

struct TimeValue
{
    TimeValue(TimePoint t, Value v) : time(t), value(v){};
    TimePoint time;
    Value value = 0.0;
};

struct Aggregate
{
    Aggregate(Value mean, Value minimum, Value maximum)
    : mean(mean), minimum(minimum), maximum(maximum)
    {
    }
    Value mean;
    Value minimum;
    Value maximum;
};

struct Row
{
    Row(TimePoint time, Duration interval, Aggregate aggregate)
    : time(time), interval(interval), aggregate(aggregate)
    {
    }
    TimePoint time;
    Duration interval;
    Aggregate aggregate;
};

enum class IntervalScope {
    OPEN_OPEN,
    OPEN_CLOSED,
    CLOSED_OPEN,
    CLOSED_CLOSED,	    // interval closed on both ends
    CLOSED_EXTENDED,	// interval extended by one element on the right end
    EXTENDED_CLOSED,
    EXTENDED_EXTENDED,
};


}
