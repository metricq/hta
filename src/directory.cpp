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

    if (config.count("metrics"))
    {
        for (const auto& metric : config["metrics"])
        {
            const auto& mode = metric["mode"];
            const auto& name = metric["name"];
            if (mode == "RW")
            {
                auto[_, added] = read_write_metrics_.emplace(
                    metric["name"], std::make_unique<ReadWriteMetric>(directory_->open(
                                        name, storage::OpenMode::read_write, Meta(metric))));
                assert(added);
                (void)added;
            }
            else if (mode == "R")
            {
                auto[_, added] = read_metrics_.emplace(
                    metric["name"], std::make_unique<ReadMetric>(directory_->open(
                                        name, storage::OpenMode::read, Meta(metric))));
                assert(added);
                (void)added;
            }
            else if (mode == "W")
            {
                auto[_, added] = write_metrics_.emplace(
                    metric["name"], std::make_unique<WriteMetric>(directory_->open(
                                        name, storage::OpenMode::write, Meta(metric))));
                assert(added);
                (void)added;
            }
            else
            {
                assert(!"Unknown metric mode");
            }
        }
    }
}

Directory::Directory(const std::filesystem::path& config_path)
: Directory(read_json_from_file(config_path))
{
}

std::vector<std::string> Directory::metric_names()
{
    return directory_->metric_names();
}

ReadWriteMetric* Directory::operator[](const std::string& name)
{
    auto it = read_write_metrics_.find(name);
    if (it == read_write_metrics_.end())
    {
        bool added;
        std::tie(it, added) = read_write_metrics_.try_emplace(
            name, std::make_unique<ReadWriteMetric>(
                      directory_->open(name, storage::OpenMode::read_write)));
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
        std::tie(it, added) = read_metrics_.try_emplace(
            name, std::make_unique<ReadMetric>(directory_->open(name, storage::OpenMode::read)));
        assert(added);
    }
    return it->second.get();
}

Directory::~Directory()
{
}
}
