#include "storage/file/directory.hpp"

#include <hta/directory.hpp>
#include <hta/exception.hpp>
#include <hta/filesystem.hpp>
#include <hta/metric.hpp>

#include <nlohmann/json.hpp>

#include <memory>
#include <string>

namespace hta
{

using json = nlohmann::json;

json read_json_from_file(const std::filesystem::path& path)
{
    std::ifstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(path);
    json config;
    config_file >> config;
    return config;
}

Directory::Directory(const json& config)
{
    auto type = config["type"].get<std::string>();
    if (type == "file")
    {
        directory_ = std::make_unique<storage::file::Directory>(config["path"].get<std::string>());
    }
    else
    {
        throw_exception("Unknown directory type: ", type);
    }
}

Directory::Directory(const std::filesystem::path& config_path)
: Directory(read_json_from_file(config_path))
{
}

ReadWriteMetric* Directory::operator[](const std::string& name)
{
    auto it = metrics_.find(name);
    if (it == metrics_.end())
    {
        bool added;
        std::tie(it, added) =
                metrics_.try_emplace(name, std::make_unique<ReadWriteMetric>((*directory_)[name]));
        assert(added);
    }
    return it->second.get();
}

ReadMetric* Directory::read_metric(const std::string& name)
{
    auto it = read_metrics_.find(name);
    if (it == read_metrics_.end())
    {
        bool added;
        std::tie(it, added) =
                read_metrics_.try_emplace(name, std::make_unique<ReadMetric>((*directory_)[name]));
        assert(added);
    }
    return it->second.get();
}

Directory::~Directory()
{
}
}
