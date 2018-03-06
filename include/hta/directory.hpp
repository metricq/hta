#pragma once

#include <hta/filesystem.hpp>

#include <nlohmann/json_fwd.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace hta
{
using json = nlohmann::json;

class ReadMetric;
class WriteMetric;
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
    ~Directory();

    ReadMetric* read_metric(const std::string& name);
    ReadWriteMetric* operator[](const std::string& name);

    std::vector<std::string> metric_names();

private:
    std::unique_ptr<storage::Directory> directory_;
    std::unordered_map<std::string, std::unique_ptr<ReadMetric>> read_metrics_;
    std::unordered_map<std::string, std::unique_ptr<WriteMetric>> write_metrics_;
    std::unordered_map<std::string, std::unique_ptr<ReadWriteMetric>> read_write_metrics_;
};
}
