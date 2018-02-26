#pragma once

#include <hta/filesystem.hpp>

#include <unordered_map>


namespace hta
{
class Metric;

namespace storage
{
    class Directory;
}

class Directory
{
public:
    Directory(const std::filesystem::path& config_path);
    Metric* operator[](const std::string& name);
    ~Directory();

private:
    std::unique_ptr<storage::Directory> directory_;
    std::unordered_map<std::string, std::unique_ptr<Metric>> metrics_;
};
}
