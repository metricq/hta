#include "directory.hpp"
#include "metric.hpp"

#include <hta/filesystem.hpp>

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
    }
}
}
