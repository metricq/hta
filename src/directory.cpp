// Copyright (c) 2018, ZIH,
// Technische Universitaet Dresden,
// Federal Republic of Germany
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of metricq nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
                auto [_, added] = read_write_metrics_.emplace(
                    metric["name"], std::make_unique<ReadWriteMetric>(directory_->open(
                                        name, storage::OpenMode::read_write, Meta(metric))));
                assert(added);
                (void)added;
            }
            else if (mode == "R")
            {
                auto [_, added] = read_metrics_.emplace(
                    metric["name"], std::make_unique<ReadMetric>(directory_->open(
                                        name, storage::OpenMode::read, Meta(metric))));
                assert(added);
                (void)added;
            }
            else if (mode == "W")
            {
                auto [_, added] = write_metrics_.emplace(
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

WriteMetric* Directory::write_metric(const std::string& name)
{
    auto it = write_metrics_.find(name);
    if (it == write_metrics_.end())
    {
        bool added;
        std::tie(it, added) = write_metrics_.try_emplace(
            name, std::make_unique<WriteMetric>(directory_->open(name, storage::OpenMode::write)));
        assert(added);
    }
    return it->second.get();
}

Directory::~Directory()
{
}
} // namespace hta
