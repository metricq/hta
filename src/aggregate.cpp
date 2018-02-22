#include <hta/types.hpp>

#include <algorithm>

namespace hta
{

Aggregate::Aggregate(Value value, Duration active_time)
: minimum(value), maximum(value), sum(value), count(1), integral(value * active_time.count()),
  active_time(active_time)
{
}

Aggregate::Aggregate(Value minimum, Value maximum, Value sum, uint64_t count, Value integral,
                     const Duration& active_time)
: minimum(minimum), maximum(maximum), sum(sum), count(count), integral(integral),
  active_time(active_time)
{
}

Aggregate& Aggregate::operator+=(Aggregate other)
{
    minimum = std::min(other.minimum, minimum);
    maximum = std::max(other.maximum, maximum);
    sum += other.sum;
    count += other.count;
    integral += other.integral;
    active_time += other.active_time;
    return *this;
}
}
