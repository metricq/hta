#pragma once

#include "hta.hpp"
#include <ostream>

namespace std
{
inline std::ostream& operator<<(std::ostream& os, hta::Duration duration)
{
    os << duration.count();
    return os;
}

inline std::ostream& operator<<(std::ostream& os, hta::TimePoint tp)
{
    os << tp.time_since_epoch();
    return os;
}
}
namespace hta
{
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
