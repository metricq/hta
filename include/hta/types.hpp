#pragma once

#include "chrono.hpp"

#include <limits>

namespace hta
{

using Value = double;

struct TimeValue
{
    TimeValue() = default;
    constexpr TimeValue(TimePoint t, Value v) : time(t), value(v){};
    TimePoint time;
    Value value;
};

constexpr inline bool operator==(TimeValue left, TimeValue right)
{
    return left.time == right.time && left.value == right.value;
}

struct Aggregate
{
    Aggregate() = default;
    Aggregate(Value value, Duration active_time = Duration(0));

    Aggregate(Value minimum, Value maximum, Value sum, uint64_t count, Value integral,
              const Duration& active_time);

    Value minimum = std::numeric_limits<double>::infinity();
    Value maximum = -std::numeric_limits<double>::infinity();
    Value sum = 0.0;
    uint64_t count = 0;
    // WARNING, this is in terms of Duration::counts
    Value integral = 0.0;
    Duration active_time = Duration(0);

    Value mean_sum() const
    {
        return sum / count;
    }

    Value mean_integral() const
    {
        return integral / active_time.count();
    }

    Aggregate& operator+=(Aggregate other);
};

struct TimeAggregate
{
    TimeAggregate() = default;
    TimeAggregate(TimePoint t, Aggregate a) : time(t), aggregate(a){};
    TimePoint time;
    Aggregate aggregate;
};

struct Row
{
    Row(Duration interval, TimePoint time, Aggregate aggregate)
    : interval(interval), time(time), aggregate(aggregate)
    {
    }
    Duration interval;
    TimePoint time;
    Aggregate aggregate;
    TimePoint end_time() const
    {
        return time + interval;
    }
};

enum class Scope
{
    open,
    closed,
    extended, // one more than the open, includes a point that is at least on or outside of the
              // border
    infinity,
};

struct IntervalScope
{
    Scope begin;
    Scope end;
};

inline TimePoint interval_begin(TimePoint time, Duration interval)
{
    Duration rem = time.time_since_epoch() % interval;
    return time - rem;
}

inline TimePoint interval_end(TimePoint time, Duration interval)
{
    return interval_begin(time, interval) + interval;
}
}
