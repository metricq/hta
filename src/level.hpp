#pragma once

#include <hta/types.hpp>

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
}
