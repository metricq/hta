#include "storage/file/directory.hpp"

#include <hta/directory.hpp>
#include <hta/exception.hpp>
#include <hta/metric.hpp>
#include <hta/filesystem.hpp>

#include <nlohmann/json.hpp>

#include <memory>
#include <string>

using json = nlohmann::json;

namespace hta
{
Directory::Directory(const std::filesystem::path& config_path)
{
    std::ifstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(config_path);
    json config;
    config_file >> config;
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

Metric* Directory::operator[](const std::string& name)
{
    auto it = metrics_.find(name);
    if (it == metrics_.end())
    {
        bool added;
        std::tie(it, added) =
            metrics_.try_emplace(name, std::make_unique<Metric>((*directory_)[name]));
        assert(added);
    }
    return it->second.get();
}

Directory::~Directory()
{
}
}
