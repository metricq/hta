#pragma once

#include <hta/filesystem.hpp>

#include <nlohmann/json_fwd.hpp>

#include <unordered_map>


namespace hta
{
using json = nlohmann::json;

class ReadWriteMetric;

namespace storage
{
    class Directory;
}

class Directory
{
public:
    Directory(const std::filesystem::path& config_path);
    Directory(const json& config);
    ReadWriteMetric* operator[](const std::string& name);
    ~Directory();

private:
    std::unique_ptr<storage::Directory> directory_;
    std::unordered_map<std::string, std::unique_ptr<ReadWriteMetric>> metrics_;
};
}
