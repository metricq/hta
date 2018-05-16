#include "directory.hpp"
#include "metric.hpp"

#include <hta/filesystem.hpp>

#include <stdexcept>
#include <string>

namespace hta::storage::file
{
Directory::Directory(const std::filesystem::path& directory) : directory_(directory)
{
}

std::unique_ptr<storage::Metric> Directory::open(const std::string& name, OpenMode mode, Meta meta)
{
    auto path = directory_ / std::filesystem::path(name);
    // Don't do that in the metric itself (member initialization before constructor body)
    std::filesystem::create_directories(path);
    switch (mode)
    {
    case OpenMode::read:
        return std::make_unique<Metric>(FileOpenTag::Read(), path);
    case OpenMode::write:
        return std::make_unique<Metric>(FileOpenTag::Write(), path, meta);
    case OpenMode::read_write:
        return std::make_unique<Metric>(FileOpenTag::ReadWrite(), path, meta);
    default:
        throw std::logic_error("Unknown OpenMode");
    }
}

std::vector<std::string> Directory::metric_names()
{
    std::vector<std::string> result;
    for (std::filesystem::path path : std::filesystem::directory_iterator(directory_))
    {
        if (std::filesystem::is_directory(path))
        {
            result.emplace_back(path.filename());
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}
} // namespace hta::storage::file
