#pragma once

#include "metric.hpp"

#include <string>

namespace hta::storage
{
class Directory
{
    virtual Metric* operator[](const std::string& name) = 0;
    virtual ~Directory(){};
};
}
