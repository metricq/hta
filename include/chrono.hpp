#pragma once

#include <chrono>

#include <cinttypes>

namespace hta
{
using Duration = std::chrono::duration<int64_t, std::nano>;

struct Clock
{
    using duration = Duration;
    using rep = duration::rep;
    using period = duration::period;
    //using time_point = std::chrono::time_point<Clock>;

    // Default constructor creates an "Invalid" timestamp that evaluates to false
    struct time_point : std::chrono::time_point<Clock> {
        using base = std::chrono::time_point<Clock>;
        using base::base;

        operator bool() const
        {
            return time_since_epoch().count() != 0;
        }
    };
    static const bool is_steady = true;
};

using TimePoint = Clock::time_point;

template <typename FromDuration, typename ToDuration = Duration>
constexpr ToDuration duration_cast(const FromDuration& dtn)
{
    return std::chrono::duration_cast<ToDuration>(dtn);
}
}
