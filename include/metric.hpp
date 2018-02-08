#pragma once

#include <types.hpp>

#include <chrono>
#include <map>
#include <memory>
#include <vector>

namespace hta
{
namespace storage
{
    class Metric;
}

class Level;

class Metric
{
public:
    Metric(std::unique_ptr<storage::Metric> storage_metric);
    ~Metric();

    void insert(TimeValue tv);
    std::vector<TimeAggregate> retrieve(TimePoint begin, TimePoint end, uint64_t min_samples = 1000,
                              IntervalScope scope = IntervalScope::CLOSED_EXTENDED);

private:
    std::vector<TimeAggregate> retrieve_raw(TimePoint begin, TimePoint end, IntervalScope scope = IntervalScope::CLOSED_EXTENDED);
    void insert(Row row);

    std::map<Duration, Level> levels_;
    // TODO make configurable
    Duration interval_min_ = duration_cast(std::chrono::seconds(10));
    uint64_t interval_factor_ = 10;
    std::unique_ptr<storage::Metric> storage_metric_;
};
}
