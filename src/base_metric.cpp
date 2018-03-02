#include "storage/metric.hpp"

#include <hta/metric.hpp>

#include <cassert>

namespace hta
{
BaseMetric::BaseMetric()
{
    // This should never be called, but is necessary for linking
    assert(false);
}

BaseMetric::BaseMetric(std::unique_ptr<storage::Metric> storage_metric)
: storage_metric_(std::move(storage_metric)), interval_min_(storage_metric_->meta().interval_min),
  interval_factor_(storage_metric_->meta().interval_factor)
{
}

BaseMetric::~BaseMetric()
{
}
}
