#include "storage/metric.hpp"

#include <hta/metric.hpp>

namespace hta
{
ReadWriteMetric::ReadWriteMetric(std::unique_ptr<storage::Metric> storage_metric)
: BaseMetric(std::move(storage_metric)), ReadMetric(), WriteMetric()
{
}
}
