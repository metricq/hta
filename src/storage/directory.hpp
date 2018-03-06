#pragma once

#include "metric.hpp"

#include <memory>
#include <string>
#include <vector>

namespace hta::storage
{
enum class OpenMode
{
    read,
    write,
    read_write,
};

class Directory
{
public:
    virtual std::unique_ptr<storage::Metric>
    open(const std::string& name, OpenMode mode = OpenMode::read_write, Meta meta = Meta()) = 0;
    virtual ~Directory(){};

    virtual std::vector<std::string> metric_names() = 0;
};
}
