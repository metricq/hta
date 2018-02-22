#pragma once

#include <hta/types.hpp>

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
                                        IntervalScope scope = IntervalScope{ Scope::extended,
                                                                             Scope::open });
    std::vector<TimeAggregate> retrieve(TimePoint begin, TimePoint end, Duration interval_max,
                                        IntervalScope scope = IntervalScope{ Scope::extended,
                                                                             Scope::open });

private:
    std::vector<TimeAggregate> retrieve_raw(TimePoint begin, TimePoint end,
                                            IntervalScope scope = IntervalScope{ Scope::closed,
                                                                                 Scope::extended });

    void insert(Row row);
    Level& get_level(Duration interval);
    Level restore_level(Duration interval);

    std::map<Duration, Level> levels_;
    // TODO make configurable
    Duration interval_min_ = duration_cast(std::chrono::seconds(10));
    uint64_t interval_factor_ = 10;
    std::unique_ptr<storage::Metric> storage_metric_;
};
}
