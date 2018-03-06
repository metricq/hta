#pragma once

#include "file.hpp"

#include "../directory.hpp"

#include <memory>
#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
namespace std
{
using namespace experimental;
}
#endif

namespace hta::storage
{
class Metric;
}

namespace hta::storage::file
{
class Metric;

class Directory : public storage::Directory
{
public:
    Directory(const std::filesystem::path& directory);
    std::unique_ptr<storage::Metric> open(const std::string& name,
                                          OpenMode mode = OpenMode::read_write,
                                          Meta meta = Meta()) override;

    std::vector<std::string> metric_names() override;

private:
    const std::filesystem::path directory_;
};
}
