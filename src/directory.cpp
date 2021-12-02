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

#include <cassert>
#include <memory>
#include <mutex>
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

Metric Directory::make_metric(const std::string& name, const hta::json& config)
{
    Mode mode = Mode::read_write;
    if (config.count("mode"))
    {
        auto mode_str = config.at("mode").get<std::string>();
        if (mode_str == "RW")
        {
            mode = Mode::read_write;
        }
        else if (mode_str == "R")
        {
            mode = Mode::read;
        }
        else if (mode_str == "W")
        {
            mode = Mode::write;
        }
        else
        {
            throw std::runtime_error(std::string("unknown metric mode ") + mode_str +
                                     " supported modes are RW,R,W");
        }
    }
    return Metric(directory_->open(name, mode, Meta(config)));
}

Directory::Directory(const json& config, bool use_mutex) : mutex_(use_mutex)
{
    auto type = config.at("type").get<std::string>();
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
        auto& metrics = config.at("metrics");
        assert(metrics.is_object());
        for (const auto& elem : metrics.items())
        {
            std::string name = elem.key();
            const auto& metric_config = elem.value();
            if (metric_config.count("prefix") && metric_config.at("prefix").get<bool>())
            {
                // TODO check for overlapping prefixes
                // add . to end
                prefixes_.emplace_back(name + ".", metric_config);
            }
            else
            {
                const auto& [metric, inserted] = emplace(name, metric_config);
                (void)metric;
                if (!inserted)
                {
                    throw_exception("duplicated metric name in initial configuration: ", name);
                }
            }
        }
    }
}

Directory::Directory(const std::filesystem::path& config_path, bool use_mutex)
: Directory(read_json_from_file(config_path), use_mutex)
{
}

std::vector<std::string> Directory::metric_names()
{
    return directory_->metric_names();
}

std::pair<Metric&, bool> Directory::emplace(const std::string& name, const json& config)
{
    std::lock_guard<OptionalMutex> guard(mutex_);

    auto [it, inserted] = metrics_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                           std::forward_as_tuple(make_metric(name, config)));
    (void)it;
    if (!inserted)
    {
        throw std::runtime_error("emplacing a metric that is already contained in a Directory "
                                 "is currently not supported");
    }
    return { it->second, inserted };
}

Metric& Directory::operator[](const std::string& name)
{
    std::lock_guard<OptionalMutex> guard(mutex_);
    if (auto it = metrics_.find(name); it != metrics_.end())
    {
        return it->second;
    }
    for (const auto& elem : prefixes_)
    {
        const auto& prefix = elem.first;
        if (prefix == name.substr(0, prefix.size()))
        {
            Metric metric = make_metric(name, elem.second);
            {
                auto it = metrics_.emplace(name, std::move(metric));
                assert(it.second);
                return it.first->second;
            }
        }
    }
    throw_exception<MissingMetricConfig>("no settings found to create metric ", name);
}

Directory::~Directory() = default;
} // namespace hta
