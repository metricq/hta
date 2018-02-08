#pragma once

#include "metric.hpp"

#include <memory>
#include <string>

namespace hta::storage
{
class Directory
{
public:
    virtual std::unique_ptr<storage::Metric> operator[](const std::string& name) = 0;
    virtual ~Directory(){};
};
}
