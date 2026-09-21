#pragma once

#include <algorithm>
#include <cstdint>

// Lower-bound on sorted raw timestamps, starting in the cached tail. Expand
// backwards exponentially before binary searching the bracket. No aggregation
// index is trusted. Subtraction/saturating growth also work above 2^32 records.
template <class Records, class Time>
uint64_t tail_lower_bound(Records& raw, Time time)
{
    uint64_t left = 0, right = raw.count;
    if (!right)
        return 0;
    uint64_t step = raw.count - raw.tail_begin();
    if (!step)
        step = std::min<uint64_t>(4096, raw.count);
    for (;;)
    {
        const auto index = raw.count - step;
        if (raw.probe(index).time < time)
        {
            left = index + 1;
            break;
        }
        right = index;
        if (!index)
            return 0;
        step += std::min(step, raw.count - step);
    }
    while (left < right)
    {
        const auto middle = left + (right - left) / 2;
        if (raw.probe(middle).time < time)
            left = middle + 1;
        else
            right = middle;
    }
    return left;
}
