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
    explicit Aggregate(Value value, Duration active_time = Duration(0));

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

    Value mean() const
    {
        if (active_time.count())
        {
            return mean_integral();
        }
        return mean_sum();
    }

    Aggregate& operator+=(Aggregate other);
};

inline Aggregate operator+(Aggregate left, Aggregate right)
{
    // taken by copy!
    left += right;
    return left;
}

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
    Row(Duration interval, TimeAggregate ta)
    : interval(interval), time(ta.time), aggregate(ta.aggregate)
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
} // namespace hta
