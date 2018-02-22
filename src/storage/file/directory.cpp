#include "directory.hpp"
#include "metric.hpp"

#include <hta/my_filesystem.hpp>

#include <string>

namespace hta::storage::file
{
Directory::Directory(const std::filesystem::path& directory) : directory_(directory)
{
}

std::unique_ptr<storage::Metric> Directory::operator[](const std::string& name)
{
    auto path = directory_ / std::filesystem::path(name);
    // Don't do that in the metric itself (member initialization before constructor body)
    std::filesystem::create_directories(path);
    return std::make_unique<Metric>(path);
}
}
