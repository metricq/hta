#pragma once

#include "hta.hpp"
#include <ostream>

namespace hta
{
inline std::ostream& operator<<(std::ostream& os, Duration duration)
{
    os << duration.count();
    return os;
}

inline std::ostream& operator<<(std::ostream& os, TimePoint tp)
{
    os << tp.time_since_epoch();
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const Aggregate& aggregate)
{
    os << "min: " << aggregate.minimum << ", max: " << aggregate.maximum
       << ", mean: " << aggregate.mean() << ", count: " << aggregate.count
       << ", active: " << aggregate.active_time;
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const Row& row)
{
    os << "[ " << row.time << " - " << row.end_time() << " ] (" << row.interval << ") "
       << row.aggregate;
    return os;
}
}
